"""End-to-end: real `hermes gateway run` + hermes-donut plugin + fake DonutChat + scripted model.

Scenario (the reported bug): the user asks for long work. The agent starts a background job and
ends its turn ("I'll report back"). When the job finishes, the gateway must wake the agent and the
result must be posted into the SAME DonutChat thread without any further user message.

usage: python -I tests/e2e_gateway.py "$(which hermes)" file://$PWD   (commit first; it installs from git)
"""
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from websockets.asyncio.server import serve

TOKEN = "donutbot_e2e"
MARKER = "JOB_RESULT_42"
posts = []  # (path, body)
llm_calls = []


class Donut(BaseHTTPRequestHandler):
    next_id = 9000

    def log_message(self, *a):
        pass

    def _send(self, body, status=200):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            return self._send({"ok": False, "error": "unauthorized"}, 401)
        self._send({"ok": True, "chats": [{"chat_id": 7, "chat_name": "Team", "chat_type": "group"}]})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        posts.append((self.path, body))
        Donut.next_id += 1
        key = "reply_id" if self.path.endswith("thread-replies") else "message_id"
        self._send({"ok": True, key: Donut.next_id})


def _sse(handler, chunks):
    handler.send_response(200)
    handler.send_header("Content-Type", "text/event-stream")
    handler.end_headers()
    for c in chunks:
        handler.wfile.write(f"data: {json.dumps(c)}\n\n".encode())
    handler.wfile.write(b"data: [DONE]\n\n")



class Model(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):  # /v1/models
        data = json.dumps({"object": "list", "data": [{"id": "fake", "object": "model"}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        msgs = req.get("messages", [])
        last = msgs[-1] if msgs else {}
        content = last.get("content")
        if isinstance(content, list):
            content = " ".join(p.get("text", "") for p in content if isinstance(p, dict))
        content = content or ""
        tool = None
        if last.get("role") == "tool":
            text = "Started the background job. I'll post the result here when it finishes."
        elif MARKER in content:
            text = f"Background job finished: {MARKER}"
        elif "start job" in content:
            tool = {"name": "terminal", "arguments": json.dumps(
                {"command": f"sleep 4; echo {MARKER}", "background": True, "notify_on_complete": True})}
            text = None
        else:
            text = "ok"
        llm_calls.append({"role": last.get("role"), "content": content[:200], "tool": bool(tool),
                          "has_tools": bool(req.get("tools"))})
        base = {"id": "c1", "object": "chat.completion.chunk", "created": int(time.time()), "model": "fake"}
        if req.get("stream"):
            if tool:
                chunks = [{**base, "choices": [{"index": 0, "delta": {"role": "assistant", "tool_calls": [
                    {"index": 0, "id": "call_1", "type": "function", "function": tool}]}, "finish_reason": None}]},
                    {**base, "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}]
            else:
                chunks = [{**base, "choices": [{"index": 0, "delta": {"role": "assistant", "content": text},
                                                 "finish_reason": None}]},
                          {**base, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}]
            chunks.append({**base, "choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 5,
                                                             "total_tokens": 15}})
            return _sse(self, chunks)
        message = {"role": "assistant", "content": text}
        if tool:
            message["tool_calls"] = [{"id": "call_1", "type": "function", "function": tool}]
        data = json.dumps({"id": "c1", "object": "chat.completion", "created": int(time.time()), "model": "fake",
                           "choices": [{"index": 0, "message": message,
                                        "finish_reason": "tool_calls" if tool else "stop"}],
                           "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def start(handler):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv.server_port


async def main(hermes, plugin_url):
    donut_port, model_port = start(Donut), start(Model)
    sent = asyncio.Event()

    async def stream(ws):
        await ws.send(json.dumps({"type": "connected", "data": {"bot_id": 42}}))
        if not sent.is_set():
            sent.set()
            await asyncio.sleep(1)
            await ws.send(json.dumps({"event_id": "evt_1", "type": "message.new", "timestamp": "2026-11-13T00:00:00Z",
                                      "data": {"message_id": 501, "chat_id": 7, "chat_name": "Team",
                                               "chat_type": "group", "text": "@hermes please start job",
                                               "sender": {"id": 9, "kind": "user", "name": "bien"},
                                               "attachments": [], "linked_messages": []}}))
        await ws.wait_closed()

    ws_server = await serve(stream, "127.0.0.1", 0)
    ws_port = ws_server.sockets[0].getsockname()[1]

    home = tempfile.mkdtemp(prefix="hermes-e2e-")
    with open(os.path.join(home, "config.yaml"), "w") as f:
        f.write(f"""model:
  default: fake
  provider: custom
  base_url: http://127.0.0.1:{model_port}/v1
  api_key: sk-fake
approvals:
  mode: "off"
""")
    with open(os.path.join(home, ".env"), "w") as f:
        f.write(f"DONUT_BOT_TOKEN={TOKEN}\nDONUT_API_URL=http://127.0.0.1:{donut_port}/bots/v1\n"
                f"DONUT_STREAM_URL=ws://127.0.0.1:{ws_port}/bots/v1/stream\nDONUT_ALLOWED_USERS=9\n"
                "OPENAI_API_KEY=sk-fake\n")
    env = {**os.environ, "HERMES_HOME": home}
    install = subprocess.run([hermes, "plugins", "install", plugin_url, "--enable", "--yes-deps"],
                             env=env, capture_output=True, text=True, timeout=120)
    print("install rc", install.returncode, install.stdout[-800:], install.stderr[-800:])
    log = open(os.path.join(home, "gateway.out"), "w")
    gw = subprocess.Popen([hermes, "gateway", "run"], env=env, stdout=log, stderr=subprocess.STDOUT)
    ok = False
    try:
        deadline = time.time() + 150
        while time.time() < deadline:
            await asyncio.sleep(1)
            if any(MARKER in json.dumps(b) and "finished" in json.dumps(b) for _, b in posts):
                ok = True
                await asyncio.sleep(2)
                break
            if gw.poll() is not None:
                break
    finally:
        gw.terminate()
        try:
            gw.wait(20)
        except subprocess.TimeoutExpired:
            gw.kill()
        ws_server.close()
    print("HERMES_HOME", home)
    print("LLM calls:")
    for c in llm_calls:
        print("  ", c)
    print("Donut posts:")
    for p, b in posts:
        print("  ", p, json.dumps(b, ensure_ascii=False)[:300])
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1], sys.argv[2])))
