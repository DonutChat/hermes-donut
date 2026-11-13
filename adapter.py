"""DonutChat gateway adapter for Hermes Agent.

Receives the bot's events over the DonutChat WebSocket stream and answers with the bot
HTTP API. Every conversation is a DonutChat thread: a top-level message starts a thread
under itself, and replies in that thread continue the same Hermes session. Because the
gateway owns delivery, results that arrive after a turn ended (background processes,
async delegation, cron, ``send_message``) are posted into the same thread.

Environment variables:
    DONUT_BOT_TOKEN         donutbot_… token (required)
    DONUT_API_URL           Bot API base (default https://api.donutchat.com/bots/v1)
    DONUT_STREAM_URL        Event stream (default: DONUT_API_URL with wss:// + /stream)
    DONUT_ALLOWED_USERS     Comma-separated DonutChat user IDs allowed to talk to Hermes
    DONUT_ALLOW_ALL_USERS   true to allow everyone the bot's trigger mode lets through
    DONUT_HOME_CHANNEL      Chat ID for cron / notification delivery
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import hashlib
import json
import logging
import mimetypes
import random
from collections import OrderedDict, deque
from pathlib import Path
from typing import Any, Optional

from gateway.config import Platform
from gateway.platforms._shared import (
    env_is_connected, get_scoped_secret, seed_extra_from_env, send_error
)
from gateway.platforms.base import (
    BasePlatformAdapter, SendResult, cache_document_from_bytes_async, cache_image_from_bytes_async
)
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.helpers import cancel_task

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "https://api.donutchat.com/bots/v1"
MAX_REPLY_BYTES = 79_000  # the API accepts 80,000 UTF-8 bytes per message or reply
MAX_ATTACHMENTS = 8
MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024
KEEPALIVE_SECONDS = 60  # the server drops a socket that sends nothing for 5 minutes
_RECONNECT_BASE, _RECONNECT_MAX = 1.0, 60.0
_DM_TYPES = {"direct", "self"}
_HUMAN_KINDS = {"user", ""}


def _api_url(config=None) -> str:
    extra = getattr(config, "extra", None) or {}
    return (get_scoped_secret("DONUT_API_URL", "") or extra.get("api_url") or DEFAULT_API_URL).rstrip("/")


def _stream_url(api_url: str, config=None) -> str:
    extra = getattr(config, "extra", None) or {}
    explicit = get_scoped_secret("DONUT_STREAM_URL", "") or extra.get("stream_url")
    if explicit:
        return explicit
    return "ws" + api_url.removeprefix("http") + "/stream"  # https→wss, http→ws


def _token(config=None) -> str:
    extra = getattr(config, "extra", None) or {}
    return (get_scoped_secret("DONUT_BOT_TOKEN", "") or getattr(config, "token", None)
            or extra.get("token") or "").strip()


def _fit(text: str) -> str:
    """Trim to the API's byte limit on a UTF-8 boundary."""
    data = text.encode()
    return text if len(data) <= MAX_REPLY_BYTES else data[:MAX_REPLY_BYTES].decode(errors="ignore")


def _byte_len(text: str) -> int:
    return len(text.encode())


class _Bounded(OrderedDict):
    """Insertion-ordered dict that forgets its oldest keys past ``limit``."""

    def __init__(self, limit: int):
        super().__init__()
        self.limit = limit

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.move_to_end(key)
        while len(self) > self.limit:
            self.popitem(last=False)


class DonutAdapter(BasePlatformAdapter):
    """Gateway adapter for DonutChat bots."""

    splits_long_messages = True  # send() chunks by UTF-8 bytes
    serial_receive_loop = True  # the stream loop awaits each event before reading the next

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("donut"))
        self._token = _token(config)
        self._api = _api_url(config)
        self._stream = _stream_url(self._api, config)
        self._http: Any = None  # httpx.AsyncClient
        self._ws_task: Optional[asyncio.Task] = None
        self._closing = False
        self._bot_id: Optional[str] = None
        self._last_event_id: Optional[str] = None
        self._seen_events: deque = deque(maxlen=2000)
        # reply_id → (chat_id, parent_message_id): edits and reply_to need the thread root.
        self._replies: _Bounded = _Bounded(5000)
        self._threads_seen: _Bounded = _Bounded(5000)  # threads whose root text Hermes was given
        self._chats: _Bounded = _Bounded(2000)  # chat_id → {"name", "type"}

    @property
    def name(self) -> str:
        return "DonutChat"

    # --- HTTP ---

    async def _post(self, path: str, body: dict) -> dict:
        resp = await self._http.post(f"{self._api}/{path}", json=body)
        try:
            data = resp.json()
        except ValueError:
            data = {}
        if resp.status_code != 200 or not data.get("ok", False):
            raise RuntimeError(f"{path}: HTTP {resp.status_code} {data.get('error') or ''}".strip())
        return data

    # --- Lifecycle ---

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        import httpx
        if not self._token:
            self._set_fatal_error("config_missing", "DONUT_BOT_TOKEN is not set", retryable=False)
            return False
        fingerprint = hashlib.sha256(self._token.encode()).hexdigest()[:16]
        if not self._acquire_platform_lock("donut", fingerprint, "DonutChat bot token"):
            return False
        self._http = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self._token}"}, timeout=30.0, follow_redirects=False)
        try:
            resp = await self._http.get(f"{self._api}/chats")
        except Exception as exc:
            await self._close_http()
            self._set_fatal_error("connect_failed", f"DonutChat API unreachable: {exc}", retryable=True)
            return False
        if resp.status_code in {401, 403}:
            await self._close_http()
            self._set_fatal_error("auth_failed", "DonutChat rejected DONUT_BOT_TOKEN", retryable=False)
            return False
        with contextlib.suppress(ValueError, AttributeError, TypeError):
            for chat in resp.json().get("chats") or []:
                self._remember_chat(chat.get("chat_id"), chat.get("chat_name"), chat.get("chat_type"))
        self._closing = False
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._mark_connected()
        self._wire_plugin_handlers(None)
        logger.info("DonutChat: connected to %s", self._api)
        return True

    async def disconnect(self) -> None:
        self._closing = True
        with contextlib.suppress(Exception):
            self._release_platform_lock()
        self._mark_disconnected()
        await cancel_task(self._ws_task)
        self._ws_task = None
        await self._close_http()

    async def _close_http(self) -> None:
        if self._http is not None:
            with contextlib.suppress(Exception):
                await self._http.aclose()
            self._http = None

    # --- Outbound ---

    def _thread_of(self, reply_to: Optional[str], metadata: Optional[dict]) -> Optional[str]:
        """Thread root for an outbound message: metadata thread_id, else reply_to resolved to its root."""
        thread = (metadata or {}).get("thread_id")
        if thread:
            return str(thread)
        if reply_to:
            known = self._replies.get(str(reply_to))
            return known[1] if known else str(reply_to)
        return None

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[dict[str, Any]] = None) -> SendResult:
        if not content:
            return SendResult(success=True)
        if self._http is None:
            return SendResult(success=False, error="Not connected")
        thread = self._thread_of(reply_to, metadata)
        result = SendResult(success=True)
        for chunk in self.truncate_message(content, MAX_REPLY_BYTES, len_fn=_byte_len):
            try:
                if thread:
                    data = await self._post("thread-replies", {
                        "chat_id": int(chat_id), "parent_message_id": int(thread),
                        "content": _fit(chunk), "content_type": "MD", "status": "OK"})
                    message_id = str(data["reply_id"])
                    self._replies[message_id] = (str(chat_id), thread)
                else:
                    data = await self._post("messages", {
                        "chat_id": int(chat_id), "content": _fit(chunk), "content_type": "MD"})
                    message_id = str(data["message_id"])
            except Exception as exc:
                logger.warning("DonutChat: send to chat %s failed: %s", chat_id, exc)
                return SendResult(success=False, error=str(exc))
            result = SendResult(success=True, message_id=message_id)
        return result

    async def edit_message(self, chat_id: str, message_id: str, content: str, *,
                           finalize: bool = False) -> SendResult:
        """Rewrite a bot thread reply (used for streamed answers). Top-level bot messages can't be edited."""
        known = self._replies.get(str(message_id))
        if known is None or self._http is None:
            return SendResult(success=False, error="Not supported")
        try:
            await self._post("thread-replies/stream", {
                "chat_id": int(known[0]), "parent_message_id": int(known[1]), "reply_id": int(message_id),
                "content": _fit(content), "status": "OK"})
        except Exception as exc:
            return SendResult(success=False, error=str(exc))
        return SendResult(success=True, message_id=str(message_id))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        if self._http is not None:
            with contextlib.suppress(Exception):
                await self._post("typing", {"chat_id": int(chat_id)})

    async def stop_typing(self, chat_id: str) -> None:
        if self._http is not None:
            with contextlib.suppress(Exception):
                await self._post("typing", {"chat_id": int(chat_id), "is_typing": False})

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        return dict(self._chats.get(str(chat_id)) or {"name": str(chat_id), "type": "group"})

    # ponytail: no bot file-upload API yet, so media sends use the base text fallback.

    # --- Inbound ---

    async def _ws_loop(self) -> None:
        from websockets.exceptions import InvalidStatus
        delay = _RECONNECT_BASE
        while not self._closing:
            try:
                if await self._listen():
                    delay = _RECONNECT_BASE
            except asyncio.CancelledError:
                return
            except InvalidStatus as exc:
                status = exc.response.status_code
                if status in {401, 403}:
                    self._set_fatal_error("auth_failed", "DonutChat rejected the bot token", retryable=False)
                    await self._notify_fatal_error()
                    return
                logger.warning("DonutChat: stream refused (HTTP %s), retrying in %.0fs", status, delay)
            except Exception as exc:
                logger.warning("DonutChat: stream error: %s, retrying in %.0fs", exc, delay)
            if self._closing:
                return
            await asyncio.sleep(delay * random.uniform(0.5, 1.5))
            delay = min(delay * 2, _RECONNECT_MAX)

    async def _listen(self) -> bool:
        """One stream connection. Returns True if it reached the live state."""
        from websockets.asyncio.client import connect
        url = self._stream
        if self._last_event_id:
            from urllib.parse import quote
            url += ("&" if "?" in url else "?") + "last_event_id=" + quote(self._last_event_id)
        live = False
        async with connect(url, additional_headers={"Authorization": f"Bearer {self._token}"},
                           max_size=2 ** 21, open_timeout=15) as ws:
            keepalive = asyncio.create_task(self._keepalive(ws))
            try:
                async for frame in ws:
                    if await self._handle_frame(frame):
                        live = True
            finally:
                await cancel_task(keepalive)
        return live

    @staticmethod
    async def _keepalive(ws) -> None:
        while True:
            await asyncio.sleep(KEEPALIVE_SECONDS)
            await ws.send("ping")

    async def _handle_frame(self, frame) -> bool:
        """Process one stream frame; True when it is the ``connected`` frame."""
        try:
            event = json.loads(frame)
        except (TypeError, ValueError):
            return False
        kind, data = event.get("type"), event.get("data") or {}
        if kind == "connected":
            self._bot_id = str(data.get("bot_id") or "") or None
            logger.info("DonutChat: stream live as bot %s", self._bot_id)
            return True
        event_id = event.get("event_id")
        if not event_id or event_id in self._seen_events:
            return False
        self._seen_events.append(event_id)
        self._last_event_id = event_id
        if kind in {"chat_added", "chat_removed"}:
            if kind == "chat_added":
                self._remember_chat(data.get("chat_id"), data.get("chat_name"), None)
            return False
        if kind in {"message.new", "thread.reply.new"}:
            try:
                await self._handle_message_event(kind, data)
            except Exception:
                logger.exception("DonutChat: failed to handle %s", kind)
        return False

    def _remember_chat(self, chat_id, name, chat_type) -> None:
        if chat_id is None:
            return
        previous = self._chats.get(str(chat_id)) or {}
        resolved = chat_type or previous.get("raw_type") or "group"
        self._chats[str(chat_id)] = {
            "name": name or previous.get("name") or str(chat_id),
            "type": "dm" if resolved in _DM_TYPES else "group", "raw_type": resolved}

    async def _handle_message_event(self, kind: str, data: dict) -> None:
        chat_id = str(int(data["chat_id"]))
        sender = data.get("sender") or {}
        text = data.get("text") or ""
        attachments = data.get("attachments") or []
        if not text.strip() and not attachments:
            return
        if kind == "message.new":
            thread_id = str(int(data["message_id"]))
            message_id = thread_id
            source_path = f"chats/{chat_id}/messages/{thread_id}"
        else:
            thread_id = str(int(data["parent_message_id"]))
            message_id = str(int(data["reply_id"]))
            source_path = f"chats/{chat_id}/messages/{thread_id}/replies/{message_id}"
            self._replies[message_id] = (chat_id, thread_id)
        self._remember_chat(chat_id, data.get("chat_name"), data.get("chat_type"))
        chat = self._chats[chat_id]

        media_urls, media_types = await self._download_attachments(attachments, source_path)
        event = MessageEvent(
            text=text, message_type=_message_type(text, media_types),
            source=self.build_source(
                chat_id=chat_id, chat_name=chat["name"], chat_type=chat["type"],
                user_id=str(sender.get("id") or ""), user_name=sender.get("name") or sender.get("username"),
                thread_id=thread_id, message_id=message_id,
                is_bot=str(sender.get("kind") or "") not in _HUMAN_KINDS),
            raw_message=data, message_id=message_id, media_urls=media_urls, media_types=media_types,
            timestamp=datetime.datetime.now())

        thread_key = f"{chat_id}:{thread_id}"
        parent = data.get("parent")
        if parent and thread_key not in self._threads_seen:
            # The thread root may predate the bot or be someone else's: give it once per thread.
            event.reply_to_message_id = thread_id
            event.reply_to_text = parent.get("text") or ""
            event.reply_to_author_name = (parent.get("sender") or {}).get("name")
        self._threads_seen[thread_key] = True
        linked = [f"[Linked message from {(ref.get('sender') or {}).get('name', 'someone')}: {ref.get('text', '')}]"
                  for ref in data.get("linked_messages") or []]
        if linked:
            event.channel_context = "\n".join(linked)
        await self.handle_message(event)

    async def _download_attachments(self, attachments: list, source_path: str) -> tuple[list[str], list[str]]:
        """Fetch attachment bytes through the authenticated content endpoint into Hermes' media cache."""
        urls: list[str] = []
        types: list[str] = []
        for attachment in attachments[:MAX_ATTACHMENTS]:
            try:
                attachment_id = int(attachment["id"])
                if attachment_id <= 0 or not 0 <= int(attachment.get("file_size") or 0) <= MAX_ATTACHMENT_BYTES:
                    continue
                # No redirects: the bearer token must never follow a storage URL.
                async with self._http.stream(
                        "GET", f"{self._api}/{source_path}/attachments/{attachment_id}/content") as resp:
                    if resp.status_code != 200:
                        continue
                    body = bytearray()
                    async for chunk in resp.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > MAX_ATTACHMENT_BYTES:
                            raise ValueError("attachment too large")
                mime = attachment.get("mime_type") or resp.headers.get("content-type", "").split(";")[0] \
                    or "application/octet-stream"
                name = Path(attachment.get("original_filename") or f"file_{attachment_id}").name
                if mime.startswith("image/"):
                    ext = Path(name).suffix or mimetypes.guess_extension(mime) or ".jpg"
                    urls.append(await cache_image_from_bytes_async(bytes(body), ext))
                else:
                    urls.append(await cache_document_from_bytes_async(bytes(body), name))
                types.append(mime)
            except Exception as exc:
                logger.warning("DonutChat: attachment %s not fetched: %s", attachment.get("id"), exc)
        return urls, types


def _message_type(text: str, media_types: list[str]) -> MessageType:
    if text.startswith("/"):
        return MessageType.COMMAND
    if any(m.startswith("image/") for m in media_types):
        return MessageType.PHOTO
    if media_types:
        return MessageType.DOCUMENT
    return MessageType.TEXT


# --- Out-of-process delivery (cron running outside the gateway) ---

async def _standalone_send(pconfig, chat_id: str, message: str, *, thread_id: Optional[str] = None,
                           media_files: Optional[list] = None, force_document: bool = False) -> dict[str, Any]:
    import httpx
    token, api = _token(pconfig), _api_url(pconfig)
    if not token:
        return send_error("DonutChat standalone send: DONUT_BOT_TOKEN is not set")
    try:
        if thread_id:
            path = "thread-replies"
            body = {"chat_id": int(chat_id), "parent_message_id": int(thread_id),
                    "content": _fit(message), "content_type": "MD", "status": "OK"}
        else:
            path = "messages"
            body = {"chat_id": int(chat_id), "content": _fit(message), "content_type": "MD"}
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
            resp = await client.post(f"{api}/{path}", json=body, headers={"Authorization": f"Bearer {token}"})
        data = resp.json() if resp.content else {}
        if resp.status_code != 200 or not data.get("ok"):
            return send_error(f"DonutChat API error ({resp.status_code}): {data.get('error') or ''}")
        return {"success": True, "platform": "donut", "chat_id": str(chat_id),
                "message_id": str(data.get("reply_id") or data.get("message_id") or "")}
    except Exception as exc:
        return send_error(f"DonutChat send failed: {exc}")


# --- Registration ---

def check_requirements() -> bool:
    try:
        import httpx  # noqa: F401
        import websockets  # noqa: F401
        return True
    except ImportError:
        return False


def validate_config(config) -> bool:
    return bool(_token(config))


def _env_enablement() -> dict | None:
    if not str(get_scoped_secret("DONUT_BOT_TOKEN", "") or "").strip():
        return None
    return seed_extra_from_env((("DONUT_API_URL", "api_url", None), ("DONUT_STREAM_URL", "stream_url", None)),
                               home_env="DONUT_HOME_CHANNEL")


def interactive_setup() -> None:
    from hermes_cli.setup import prompt, prompt_yes_no, save_env_value, print_header, print_info, print_success
    from hermes_cli.setup_platforms import declines_reconfigure
    print_header("DonutChat")
    if declines_reconfigure("DonutChat", "Reconfigure DonutChat?", "DONUT_BOT_TOKEN"):
        return
    print_info("Create a bot in DonutChat (Settings → My bots), then copy its donutbot_… token.")
    token = prompt("Bot token", password=True)
    if not token:
        return
    save_env_value("DONUT_BOT_TOKEN", token.strip())
    print_info("Your DonutChat user ID is shown in Settings → Account.")
    if prompt_yes_no("Allow everyone the bot's trigger mode lets through?", False):
        save_env_value("DONUT_ALLOW_ALL_USERS", "true")
    else:
        save_env_value("DONUT_ALLOW_ALL_USERS", "false")
        allowed = prompt("Allowed DonutChat user IDs (comma-separated)")
        save_env_value("DONUT_ALLOWED_USERS", (allowed or "").replace(" ", ""))
    home = prompt("Home chat ID for cron results (optional)")
    if home:
        save_env_value("DONUT_HOME_CHANNEL", home.strip())
    print_success("DonutChat configuration saved. Restart the gateway: hermes gateway restart")


_PLATFORM_HINT = (
    "You are talking with your user in DonutChat. Each conversation is a DonutChat thread, and "
    "your replies are posted in it as Markdown (headings, lists, tables and code blocks render). "
    "For long work, start background processes or async delegation instead of blocking: when "
    "they finish you are woken in this same thread and your reply is delivered there, so you can "
    "say you will report back and actually do it.")


def register(ctx) -> None:
    """Plugin entry point."""
    ctx.register_platform(
        name="donut", label="DonutChat", adapter_factory=DonutAdapter,
        check_fn=check_requirements, validate_config=validate_config,
        is_connected=env_is_connected("DONUT_BOT_TOKEN"), required_env=["DONUT_BOT_TOKEN"],
        install_hint="No extra packages needed (httpx and websockets ship with Hermes)",
        setup_fn=interactive_setup, env_enablement_fn=_env_enablement,
        cron_deliver_env_var="DONUT_HOME_CHANNEL", standalone_sender_fn=_standalone_send,
        allowed_users_env="DONUT_ALLOWED_USERS", allow_all_env="DONUT_ALLOW_ALL_USERS",
        max_message_length=20_000, emoji="🍩", allow_update_command=True,
        dashboard_description="Answer DonutChat messages in threads, including results of long-running work.",
        docs_url="https://github.com/DonutChat/hermes-donut", platform_hint=_PLATFORM_HINT)
