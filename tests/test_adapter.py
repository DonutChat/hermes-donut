"""Tests against the real Hermes gateway base classes and a fake DonutChat server.

Run from a Hermes checkout's environment:  python -m pytest tests
"""

import asyncio
import importlib.util
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from websockets.asyncio.server import serve

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "donutbot_test"


def _load_plugin():
    spec = importlib.util.spec_from_file_location(
        "hermes_donut", ROOT / "__init__.py", submodule_search_locations=[str(ROOT)])
    module = importlib.util.module_from_spec(spec)
    sys.modules["hermes_donut"] = module
    spec.loader.exec_module(module)
    return sys.modules["hermes_donut.adapter"]


adapter_mod = _load_plugin()


class _Ctx:
    """Stand-in for the plugin context: registers into the real platform registry."""

    def __init__(self):
        self.kwargs = None

    def register_platform(self, **kwargs):
        from gateway.platform_registry import PlatformEntry, platform_registry
        self.kwargs = kwargs
        platform_registry.register(PlatformEntry(source="plugin", **kwargs))


CTX = _Ctx()
adapter_mod.register(CTX)


class FakeDonut:
    """Bot REST API on a thread + event stream on the test loop."""

    def __init__(self):
        self.requests = []  # (path, json body or None, auth header)
        self.frames = []  # sent to each stream connection after it opens
        self.stream_queries = []
        self.attachments = {}  # path → bytes
        self.next_id = 1000
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _reply(self, status, body, ctype="application/json"):
                data = body if isinstance(body, bytes) else json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                fake.requests.append((self.path, None, self.headers.get("Authorization")))
                if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                    return self._reply(401, {"ok": False, "error": "unauthorized"})
                if self.path == "/bots/v1/chats":
                    return self._reply(200, {"ok": True, "chats": [
                        {"chat_id": 7, "chat_name": "Team", "chat_type": "group"}]})
                if self.path in fake.attachments:
                    return self._reply(200, fake.attachments[self.path], "image/png")
                self._reply(404, {"ok": False, "error": "not_found"})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                fake.requests.append((self.path, body, self.headers.get("Authorization")))
                fake.next_id += 1
                if self.path == "/bots/v1/thread-replies":
                    return self._reply(200, {"ok": True, "reply_id": fake.next_id})
                if self.path == "/bots/v1/messages":
                    return self._reply(200, {"ok": True, "message_id": fake.next_id})
                self._reply(200, {"ok": True})

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.api = f"http://127.0.0.1:{self.http.server_port}/bots/v1"

    async def start_stream(self):
        async def handler(ws):
            self.stream_queries.append(ws.request.path)
            if ws.request.headers.get("Authorization") != f"Bearer {TOKEN}":
                await ws.close()
                return
            await ws.send(json.dumps({"type": "connected", "data": {"bot_id": 42}}))
            for frame in self.frames:
                await ws.send(json.dumps(frame))
            await ws.wait_closed()

        self.ws = await serve(handler, "127.0.0.1", 0)
        port = self.ws.sockets[0].getsockname()[1]
        return f"ws://127.0.0.1:{port}/bots/v1/stream"

    def posts(self, path):
        return [body for p, body, _ in self.requests if p == f"/bots/v1/{path}"]

    async def close(self):
        self.http.shutdown()
        if getattr(self, "ws", None):
            self.ws.close()
            await self.ws.wait_closed()


@pytest.fixture
async def fake():
    server = FakeDonut()
    yield server
    await server.close()


@pytest.fixture
def make_adapter(fake, monkeypatch):
    from gateway.config import PlatformConfig

    async def factory(stream_url="ws://127.0.0.1:9/unused"):
        monkeypatch.setenv("DONUT_BOT_TOKEN", TOKEN)
        monkeypatch.setenv("DONUT_API_URL", fake.api)
        monkeypatch.setenv("DONUT_STREAM_URL", stream_url)
        adapter = adapter_mod.DonutAdapter(PlatformConfig(enabled=True))
        adapter._acquire_platform_lock = lambda *a: True
        adapter._release_platform_lock = lambda *a: None
        events = []

        async def capture(event):
            events.append(event)

        adapter.handle_message = capture
        adapter.events = events
        return adapter

    return factory


def _message_new(event_id="evt_1", text="hello", **extra):
    data = {"message_id": 501, "chat_id": 7, "chat_name": "Team", "chat_type": "group",
            "sender": {"id": 9, "kind": "user", "name": "bien", "username": "bien"},
            "text": text, "attachments": [], "linked_messages": [], **extra}
    return {"event_id": event_id, "type": "message.new", "data": data}


def _thread_reply(event_id, reply_id, text="more", parent=None):
    data = {"reply_id": reply_id, "parent_message_id": 501, "chat_id": 7, "chat_name": "Team",
            "chat_type": "group", "sender": {"id": 9, "kind": "user", "name": "bien"},
            "text": text, "attachments": [], "linked_messages": []}
    if parent:
        data["parent"] = parent
    return {"event_id": event_id, "type": "thread.reply.new", "data": data}


def test_register_declares_delivery_hooks():
    kwargs = CTX.kwargs
    assert kwargs["name"] == "donut"
    assert kwargs["cron_deliver_env_var"] == "DONUT_HOME_CHANNEL"
    assert kwargs["standalone_sender_fn"] is adapter_mod._standalone_send
    assert kwargs["allowed_users_env"] == "DONUT_ALLOWED_USERS"
    from gateway.config import Platform
    assert Platform("donut").value == "donut"


def test_tool_progress_defaults_on_and_user_config_wins():
    from gateway.display_config import resolve_display_setting
    assert resolve_display_setting({}, "donut", "tool_progress") == "all"
    assert resolve_display_setting({}, "donut", "tool_progress_grouping") == "accumulate"
    off = {"display": {"platforms": {"donut": {"tool_progress": "off"}}}}
    assert resolve_display_setting(off, "donut", "tool_progress") == "off"
    assert resolve_display_setting({"display": {"tool_progress": "off"}}, "donut", "tool_progress") == "off"


async def test_stream_events_become_threaded_message_events(fake, make_adapter):
    fake.frames = [_message_new(), _message_new()]  # duplicate event_id is dropped
    stream = await fake.start_stream()
    adapter = await make_adapter(stream)
    assert await adapter.connect()
    for _ in range(100):
        if adapter.events:
            break
        await asyncio.sleep(0.02)
    await asyncio.sleep(0.1)
    await adapter.disconnect()

    assert len(adapter.events) == 1
    event = adapter.events[0]
    assert event.text == "hello"
    assert event.source.chat_id == "7"
    assert event.source.thread_id == "501"  # a top-level message starts a thread under itself
    assert event.source.chat_type == "group"
    assert event.source.user_id == "9"
    assert not event.source.is_bot
    assert adapter._bot_id == "42"
    assert adapter._last_event_id == "evt_1"


async def test_reconnect_resumes_from_last_event(fake, make_adapter):
    stream = await fake.start_stream()
    adapter = await make_adapter(stream)
    adapter._last_event_id = "evt_9"
    task = asyncio.create_task(adapter._listen())
    for _ in range(100):
        if fake.stream_queries:
            break
        await asyncio.sleep(0.02)
    task.cancel()
    assert fake.stream_queries[0].endswith("?last_event_id=evt_9")


async def test_thread_reply_gets_root_context_once(make_adapter):
    adapter = await make_adapter()
    parent = {"message_id": 501, "sender": {"name": "alice"}, "text": "Fix the scroll bug"}
    await adapter._handle_frame(json.dumps(_thread_reply("e1", 601, parent=parent)))
    await adapter._handle_frame(json.dumps(_thread_reply("e2", 602, parent=parent)))

    first, second = adapter.events
    assert first.source.thread_id == "501" and first.message_id == "601"
    assert first.reply_to_text == "Fix the scroll bug" and first.reply_to_author_name == "alice"
    assert second.reply_to_text is None
    assert adapter._replies["602"] == ("7", "501")


async def test_mention_before_command_is_stripped(make_adapter):
    adapter = await make_adapter()
    await adapter._handle_frame(json.dumps(_message_new("a", "@hermes /approve", mentions_bot=True)))
    await adapter._handle_frame(json.dumps(_message_new("b", "@hermes look at /tmp/x", mentions_bot=True)))
    assert adapter.events[0].text == "/approve"
    assert adapter.events[0].message_type.name == "COMMAND"
    assert adapter.events[1].text == "@hermes look at /tmp/x"


async def test_bot_senders_are_marked(make_adapter):
    adapter = await make_adapter()
    frame = _message_new(sender={"id": 3, "kind": "bot", "name": "other"})
    await adapter._handle_frame(json.dumps(frame))
    assert adapter.events[0].source.is_bot


async def test_send_threads_and_edits(fake, make_adapter):
    adapter = await make_adapter()
    assert await adapter.connect()

    # Gateway reply to a turn: metadata carries the thread root.
    result = await adapter.send("7", "Done.", metadata={"thread_id": "501"})
    assert result.success
    assert fake.posts("thread-replies")[-1] == {
        "chat_id": 7, "parent_message_id": 501, "content": "Done.", "content_type": "MD", "status": "OK"}

    # Streamed answers are edits of that reply, addressed by its thread.
    edited = await adapter.edit_message("7", result.message_id, "Done. All checks passed.", finalize=True)
    assert edited.success
    assert fake.posts("thread-replies/stream")[-1] == {
        "chat_id": 7, "parent_message_id": 501, "reply_id": int(result.message_id),
        "content": "Done. All checks passed.", "status": "OK"}

    # reply_to pointing at a thread reply resolves to the thread root.
    adapter._replies["601"] = ("7", "501")
    await adapter.send("7", "Follow-up", reply_to="601")
    assert fake.posts("thread-replies")[-1]["parent_message_id"] == 501

    # No thread: a top-level message (cron / home channel).
    await adapter.send("7", "Daily report")
    assert fake.posts("messages")[-1] == {"chat_id": 7, "content": "Daily report", "content_type": "MD"}
    assert not (await adapter.edit_message("7", "999999", "x")).success
    await adapter.disconnect()


async def test_long_answers_split_under_byte_limit(fake, make_adapter):
    adapter = await make_adapter()
    assert await adapter.connect()
    text = "\n\n".join(["가나다라마바사아자차카타파하 " * 100] * 30)  # ~130 KB of UTF-8
    assert (await adapter.send("7", text, metadata={"thread_id": "501"})).success
    bodies = fake.posts("thread-replies")
    assert len(bodies) >= 2
    assert all(len(b["content"].encode()) <= 80_000 for b in bodies)
    await adapter.disconnect()


async def test_image_attachment_is_cached(fake, make_adapter):
    png = b"\x89PNG\r\n\x1a\n" + b"0" * 64
    fake.attachments["/bots/v1/chats/7/messages/501/attachments/77/content"] = png
    adapter = await make_adapter()
    assert await adapter.connect()
    frame = _message_new(text="see this", attachments=[
        {"id": 77, "file_type": "image", "mime_type": "image/png", "original_filename": "s.png", "file_size": len(png)}])
    await adapter._handle_frame(json.dumps(frame))
    await adapter.disconnect()

    event = adapter.events[0]
    assert event.media_types == ["image/png"]
    assert Path(event.media_urls[0]).read_bytes() == png
    assert event.message_type.name == "PHOTO"


async def test_bad_token_is_fatal(fake, make_adapter, monkeypatch):
    adapter = await make_adapter()
    adapter._token = "donutbot_wrong"
    assert not await adapter.connect()
    assert adapter.has_fatal_error and not adapter._fatal_error_retryable


async def test_standalone_send_posts_into_thread(fake, monkeypatch):
    from gateway.config import PlatformConfig
    monkeypatch.setenv("DONUT_BOT_TOKEN", TOKEN)
    monkeypatch.setenv("DONUT_API_URL", fake.api)
    result = await adapter_mod._standalone_send(PlatformConfig(enabled=True), "7", "cron done", thread_id="501")
    assert result["success"]
    assert fake.posts("thread-replies")[-1]["parent_message_id"] == 501
