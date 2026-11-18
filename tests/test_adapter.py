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


def _parse_multipart(content_type, raw):
    from email.parser import BytesParser
    from email.policy import HTTP
    msg = BytesParser(policy=HTTP).parsebytes(b"Content-Type: " + content_type.encode() + b"\r\n\r\n" + raw)
    return [(part.get_param("name", header="content-disposition"), part.get_filename(),
             part.get_payload(decode=True)) for part in msg.iter_parts()]


class FakeDonut:
    """Bot REST API on a thread + event stream on the test loop."""

    def __init__(self):
        self.requests = []  # (path, json body or None, auth header)
        self.frames = []  # sent to each stream connection after it opens
        self.stream_queries = []
        self.attachments = {}  # path → bytes
        self.uploads = []  # parsed multipart posts
        self.uploads_enabled = True
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
                raw = self.rfile.read(int(self.headers["Content-Length"]))
                if self.headers.get("Content-Type", "").startswith("multipart/form-data"):
                    parts = _parse_multipart(self.headers["Content-Type"], raw)
                    fake.uploads.append(parts)
                    if not fake.uploads_enabled:
                        return self._reply(403, {"ok": False, "error": "uploads_disabled"})
                    fake.next_id += 1
                    return self._reply(200, {"ok": True, "reply_id": fake.next_id})
                body = json.loads(raw)
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


def test_tool_progress_defaults_off_and_user_config_wins():
    from gateway.display_config import resolve_display_setting
    assert resolve_display_setting({}, "donut", "tool_progress") == "off"
    assert resolve_display_setting({}, "donut", "tool_progress_grouping") == "accumulate"
    on = {"display": {"platforms": {"donut": {"tool_progress": "all"}}}}
    assert resolve_display_setting(on, "donut", "tool_progress") == "all"
    assert resolve_display_setting({"display": {"tool_progress": "new"}}, "donut", "tool_progress") == "new"


def test_interim_messages_default_off_and_user_config_wins():
    from gateway.display_config import resolve_display_setting
    assert resolve_display_setting({}, "donut", "interim_assistant_messages") is False
    on = {"display": {"platforms": {"donut": {"interim_assistant_messages": True}}}}
    assert resolve_display_setting(on, "donut", "interim_assistant_messages") is True


def test_hard_breaks_keep_lines_but_not_code_or_paragraphs():
    hb = adapter_mod._hard_breaks
    assert hb("💻 Running a\n📚 Reading b\n🐍 Running c") == "💻 Running a  \n📚 Reading b  \n🐍 Running c"
    assert hb("para one\n\npara two") == "para one\n\npara two"
    code = "```\nx = 1\ny = 2\n```"
    assert hb("intro\n" + code) == "intro  \n" + code
    assert hb(hb("a\nb")) == "a  \nb"


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


async def test_typing_wire_scopes(fake, make_adapter):
    adapter = await make_adapter(await fake.start_stream())
    assert await adapter.connect()
    root = 9223372036854775807
    try:
        for metadata in ({"thread_id": str(root)}, None, {"thread_id": "0"}):
            await adapter.send_typing("7", metadata)
            await adapter._stop_typing_with_metadata("7", metadata)
        for invalid in ("wrong", "-1", str(root + 1), float(root), True):
            await adapter.send_typing("7", {"thread_id": invalid})
            await adapter.stop_typing("7", {"thread_id": invalid})
    finally:
        await adapter.disconnect()
    expected = [
        {"chat_id": 7, "is_typing": active, **scope}
        for scope in ({"parent_message_id": root}, {}, {})
        for active in (True, False)
    ]
    assert [(body, auth) for path, body, auth in fake.requests if path == "/bots/v1/typing"] == [
        (body, f"Bearer {TOKEN}") for body in expected
    ]


async def _wait_until(predicate):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("outcome", ["complete", "fail", "cancel", "stop"])
async def test_gateway_typing_lifecycle_is_scoped(fake, make_adapter, monkeypatch, outcome):
    from gateway.platforms.base import BasePlatformAdapter, _thread_metadata_for_event

    adapter = await make_adapter(await fake.start_stream())
    adapter.handle_message = BasePlatformAdapter.handle_message.__get__(adapter)
    root, sibling = 9007199254740993, 9007199254740995
    release_turn, release_send, in_send = asyncio.Event(), asyncio.Event(), asyncio.Event()
    hold_others = asyncio.Event()
    sessions = {}
    keep_typing, send = adapter._keep_typing, adapter.send

    async def fast_refresh(chat_id, metadata=None, stop_event=None):
        await keep_typing(chat_id, interval=0.05, metadata=metadata, stop_event=stop_event)

    async def handler(event):
        key = adapter._event_session_key(event)
        if event.get_command() == "stop":
            # Stand in for the runner's authorized stop handler, but exercise the real
            # base adapter's inline dispatch and command-guard swap.
            await adapter.interrupt_session_activity(key, event.source.chat_id,
                                                     metadata=_thread_metadata_for_event(event))
            return "stopped"
        scope = int(event.source.thread_id) if event.source.thread_id else None
        sessions[scope] = adapter._session_tasks[key]
        if scope != root:
            await hold_others.wait()
            return None
        await release_turn.wait()
        if outcome == "fail":
            raise RuntimeError("turn failed")
        return "finished"

    async def slow_send(chat_id, content, **kwargs):
        if content in {"finished", "stopped"}:
            in_send.set()
            await release_send.wait()
        return await send(chat_id, content, **kwargs)

    def states(scope):
        return [body["is_typing"] for body in fake.posts("typing")
                if body.get("parent_message_id") == scope]

    monkeypatch.setattr(adapter, "_keep_typing", fast_refresh)
    monkeypatch.setattr(adapter, "send", slow_send)
    adapter.set_message_handler(handler)
    assert await adapter.connect()
    stop_command = None
    try:
        # A main-chat source can coexist with threads even though Donut inbound messages
        # are always threaded. Use the real frame dispatcher for both kinds of thread turn.
        await adapter.handle_message(adapter_mod.MessageEvent(
            text="chat", message_type=adapter_mod.MessageType.TEXT,
            source=adapter.build_source("7", chat_type="group", user_id="9")))
        await adapter._handle_frame(json.dumps(_message_new("root", message_id=root)))
        sibling_frame = _thread_reply("sibling", 601)
        sibling_frame["data"]["parent_message_id"] = sibling
        await adapter._handle_frame(json.dumps(sibling_frame))
        await _wait_until(lambda: all(True in states(scope) for scope in (None, root, sibling)))

        if outcome == "stop":
            stop_frame = _thread_reply("stop", 602, "/stop")
            stop_frame["data"]["parent_message_id"] = root
            stop_command = asyncio.create_task(adapter._handle_frame(json.dumps(stop_frame)))
        elif outcome == "cancel":
            sessions[root].cancel()
        else:
            release_turn.set()
        if outcome in {"complete", "stop"}:
            await asyncio.wait_for(in_send.wait(), 5)
            if outcome == "stop":
                assert False in states(root), "clear must precede the blocked /stop confirmation"
        else:
            await _wait_until(sessions[root].done)

        root_count, chat_count, sibling_count = map(lambda scope: len(states(scope)), (root, None, sibling))
        # The other scopes must still refresh even while send or command confirmation is stalled.
        await _wait_until(lambda: len(states(None)) >= chat_count + 3 and
                          len(states(sibling)) >= sibling_count + 3)
        assert all(states(None)) and all(states(sibling))
        assert True not in states(root)[root_count:]
        release_send.set()
        if stop_command is not None:
            await asyncio.wait_for(stop_command, 5)
        await _wait_until(sessions[root].done)
        assert states(root)[-1] is False
        assert all(body["chat_id"] == 7 for body in fake.posts("typing"))
        if outcome in {"complete", "stop"}:
            assert [(body["parent_message_id"], body["content"]) for body in fake.posts("thread-replies")] == [
                (root, "finished" if outcome == "complete" else "stopped")
            ]
    finally:
        release_send.set()
        if stop_command is not None and not stop_command.done():
            await adapter_mod.cancel_task(stop_command)
        await adapter.cancel_background_tasks()
        await adapter.disconnect()
    assert not adapter._typing_paused
    assert not adapter._typing_turns
    assert states(None)[-1] is False and states(sibling)[-1] is False


async def test_typing_stop_is_bounded_and_preserves_cancellation(make_adapter, monkeypatch):
    adapter = await make_adapter()
    adapter._http = object()
    requested = asyncio.Event()
    unwound = asyncio.Event()

    async def slow_post(path, body):
        requested.set()
        try:
            await asyncio.Future()
        finally:
            unwound.set()

    monkeypatch.setattr(adapter, "_post", slow_post)
    monkeypatch.setattr(adapter_mod, "_TYPING_TIMEOUT", 0.05)
    await asyncio.wait_for(adapter.stop_typing("7", {"thread_id": "501"}), 1)
    assert requested.is_set() and unwound.is_set()

    requested.clear()
    unwound.clear()
    monkeypatch.setattr(adapter_mod, "_TYPING_TIMEOUT", 5)
    stop = asyncio.create_task(adapter.stop_typing("7", {"thread_id": "501"}))
    try:
        await asyncio.wait_for(requested.wait(), 1)
    finally:
        stop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stop
    assert unwound.is_set()


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


async def test_files_upload_into_the_thread(fake, make_adapter, tmp_path):
    adapter = await make_adapter()
    assert await adapter.connect()
    shot = tmp_path / "shot.png"
    shot.write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 32)

    result = await adapter.send_image_file("7", str(shot), caption="Here it is", metadata={"thread_id": "501"})
    assert result.success
    (payload_part, file_part), = fake.uploads
    assert payload_part[0] == "payload"
    assert json.loads(payload_part[2]) == {"chat_id": 7, "parent_message_id": 501, "content": "Here it is",
                                           "content_type": "MD", "status": "OK"}
    assert file_part[:2] == ("file", "shot.png") and file_part[2] == shot.read_bytes()
    assert adapter._replies[result.message_id] == ("7", "501")

    # Uploads not allowed by the owner: falls back to Hermes' notice instead of failing silently.
    fake.uploads_enabled = False
    doc = tmp_path / "report.pdf"
    doc.write_bytes(b"%PDF-1.7\n")
    fallback = await adapter.send_document("7", str(doc), metadata={"thread_id": "501"})
    assert len(fake.uploads) == 2
    assert fake.posts("thread-replies"), "the fallback notice is posted as a normal reply"

    # No thread (e.g. home channel): no upload attempt.
    await adapter.send_document("7", str(doc))
    assert len(fake.uploads) == 2
    await adapter.disconnect()


@pytest.fixture
def home_config(tmp_path, monkeypatch):
    from hermes_cli import config
    path = tmp_path / "config.yaml"
    monkeypatch.setattr(config, "get_config_path", lambda: path)
    monkeypatch.delenv("DONUT_HOME_CHANNEL", raising=False)
    return path


@pytest.mark.parametrize("in_thread", [False, True])
async def test_first_authorized_message_sets_main_chat_home(make_adapter, home_config, in_thread):
    from gateway.config import HomeChannel
    from hermes_cli.config import load_config
    adapter = await make_adapter()
    adapter.set_authorization_check(lambda *args, **kwargs: True)
    frame = _thread_reply("home", 601) if in_thread else _message_new()
    await adapter._handle_frame(json.dumps(frame))
    home = adapter.config.home_channel
    assert isinstance(home, HomeChannel)
    assert home.chat_id == "7"
    assert home.thread_id is None  # notices go to the main chat, not the triggering thread
    assert load_config()["platforms"]["donut"]["home_channel"] == home.to_dict()
    assert adapter.events[0].source.thread_id == "501"  # normal answers stay threaded
    await adapter._handle_frame(json.dumps(_message_new("other", chat_id=8)))
    assert adapter.config.home_channel.chat_id == "7"


@pytest.mark.parametrize("authorization", [False, None])
async def test_home_not_set_without_authorization(make_adapter, home_config, authorization):
    adapter = await make_adapter()
    adapter.set_authorization_check(lambda *args, **kwargs: authorization)
    await adapter._handle_frame(json.dumps(_message_new()))
    assert adapter.config.home_channel is None
    assert not home_config.exists()


async def test_home_respects_admin_policy_and_ignores_bots(make_adapter, home_config):
    adapter = await make_adapter()
    adapter.set_authorization_check(lambda *args, **kwargs: True)
    adapter.config.extra["group_allow_admin_from"] = ["99"]
    await adapter._handle_frame(json.dumps(_message_new()))
    assert not home_config.exists()
    adapter.config.extra.clear()
    await adapter._handle_frame(json.dumps(_message_new("bot", sender={"id": 99, "kind": "bot"})))
    assert not home_config.exists()


@pytest.mark.parametrize("existing", ["config", "env"])
async def test_explicit_home_is_preserved(make_adapter, home_config, monkeypatch, existing):
    from gateway.config import HomeChannel, Platform
    adapter = await make_adapter()
    adapter.set_authorization_check(lambda *args, **kwargs: True)
    if existing == "config":
        adapter.config.home_channel = HomeChannel(Platform("donut"), "88", "Chosen", thread_id="89")
    else:
        monkeypatch.setenv("DONUT_HOME_CHANNEL", "88")
    await adapter._handle_frame(json.dumps(_message_new()))
    assert not home_config.exists()
    if existing == "config":
        assert adapter.config.home_channel.chat_id == "88"
        assert adapter.config.home_channel.thread_id == "89"


async def test_home_save_failure_does_not_drop_request(make_adapter, home_config, monkeypatch):
    from gateway import config
    adapter = await make_adapter()
    adapter.set_authorization_check(lambda *args, **kwargs: True)
    def fail(*args, **kwargs):
        raise OSError("read-only config")
    monkeypatch.setattr(config, "persist_home_channel", fail)
    await adapter._handle_frame(json.dumps(_message_new()))
    assert len(adapter.events) == 1
    assert adapter.config.home_channel is None


async def test_saved_home_reloads_and_delivers_to_main_chat(fake, make_adapter, home_config):
    from gateway.config import PlatformConfig
    from hermes_cli.config import load_config
    adapter = await make_adapter()
    adapter.set_authorization_check(lambda *args, **kwargs: True)
    await adapter._handle_frame(json.dumps(_thread_reply("home", 601)))
    restored = PlatformConfig.from_dict(load_config()["platforms"]["donut"])
    restarted = adapter_mod.DonutAdapter(restored)
    restarted._acquire_platform_lock = lambda *args: True
    restarted._release_platform_lock = lambda: None
    try:
        assert await restarted.connect()
        home = restarted.config.home_channel
        assert home.chat_id == "7" and home.thread_id is None
        result = await restarted.send(home.chat_id, "Scheduled result", metadata={"thread_id": home.thread_id})
        assert result.success
        assert fake.posts("messages")[0]["chat_id"] == 7
        assert not fake.posts("thread-replies")
    finally:
        await restarted.disconnect()


async def test_manual_sethome_still_reaches_gateway(make_adapter, home_config):
    adapter = await make_adapter()
    adapter.set_authorization_check(lambda *args, **kwargs: True)
    await adapter._handle_frame(json.dumps(_message_new(text="/sethome")))
    assert adapter.events[0].get_command() == "sethome"
    assert not home_config.exists()  # the gateway handles the explicit command and its target


@pytest.mark.parametrize(("text", "language"), [
    ("현재 작업 진행 상황을 한국어로 알려주세요", "ko"),
    ("Please search the web and explain the results in detail", "en"),
    ("この作業の進捗を教えてください", "ja"),
    ("Por favor busca información sobre este tema y explica los resultados", "es"),
    ("Veuillez rechercher les informations et expliquer les résultats", "fr"),
    ("ok", None),
    ("/approve", None),
    ("```python\nprint('현재 작업 진행 상황을 알려주세요')\n```", None),
    ("https://example.com/현재작업진행상황을알려주세요", None),
])
def test_detects_only_confident_user_prose(text, language):
    assert adapter_mod.detect_language(text) == language


async def test_language_follows_sender_and_thread_without_touching_output(make_adapter):
    adapter = await make_adapter()
    await adapter._handle_frame(json.dumps(_message_new("lang1", "현재 작업 진행 상황을 알려주세요")))
    await adapter._handle_frame(json.dumps(_thread_reply("lang2", 602, "ok")))
    other = _thread_reply("lang3", 603, "ok")
    other["data"]["sender"]["id"] = 10
    await adapter._handle_frame(json.dumps(other))
    await adapter._handle_frame(json.dumps(_message_new("lang4", "ok", message_id=502)))
    await adapter._handle_frame(json.dumps(_thread_reply("lang5", 605,
        "Please search the web and explain the results in detail")))
    await adapter._handle_frame(json.dumps(_thread_reply("lang6", 606, "/approve")))
    assert [e.source.language for e in adapter.events] == ["ko", "ko", None, None, "en", "en"]
    assert adapter.events[0].text == "현재 작업 진행 상황을 알려주세요"
    from gateway.session import SessionSource
    assert SessionSource.from_dict(adapter.events[-1].source.to_dict()).language == "en"


async def test_language_does_not_use_thread_root_or_linked_messages(make_adapter):
    adapter = await make_adapter()
    frame = _thread_reply("lang1", 601, "ok", parent={
        "text": "현재 작업 진행 상황을 한국어로 알려주세요", "sender": {"name": "someone else"}})
    frame["data"]["linked_messages"] = [{"text": "この作業の進捗を教えてください"}]
    await adapter._handle_frame(json.dumps(frame))
    assert adapter.events[0].source.language is None


async def test_real_gateway_delivery_localizes_progress_without_translating_commands(fake, make_adapter):
    from agent.display import get_tool_verb
    from gateway.platforms.base import BasePlatformAdapter
    from gateway.session import build_session_key

    adapter = await make_adapter()
    assert await adapter.connect()
    try:
        command = "python check_status.py --language=en"
        async def handler(event):
            return f"💻 {await asyncio.to_thread(get_tool_verb, 'terminal')} `{command}`"
        adapter.set_message_handler(handler)
        for frame in [_message_new("ko", "현재 작업 진행 상황을 알려주세요"),
                      _message_new("en", "Please check the current status of the background job", message_id=502)]:
            await adapter._handle_frame(json.dumps(frame))
        await asyncio.gather(*(BasePlatformAdapter._process_message_background(
            adapter, event, build_session_key(event.source)) for event in adapter.events))
        replies = fake.posts("thread-replies")
        by_thread = {str(body["parent_message_id"]): body["content"] for body in replies}
        assert "실행 중" in by_thread["501"]
        assert "Running" in by_thread["502"]
        assert all(command in body for body in by_thread.values())
    finally:
        await adapter.disconnect()


def test_hermes_installer_discovers_language_dependency():
    from pm.plugin_declarations import read_python_declaration
    declaration = read_python_declaration(ROOT)
    assert "langdetect>=1.0.9,<2" in declaration.install_requirements
