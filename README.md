# hermes-donut

DonutChat platform plugin for [Hermes Agent](https://github.com/NousResearch/hermes-agent). It connects a
DonutChat bot to the Hermes gateway, so Hermes answers in DonutChat threads the same way it does in Slack or
Discord.

Because the gateway owns delivery, work that finishes after a turn has ended still reaches the thread:

- background processes started with `notify_on_complete`
- async delegation results
- cron jobs (`deliver=donut`)
- the `send_message` tool

The older `donut_hermes.py` bridge calls Hermes' API server once per message. A result that arrives after
that call returns has no route back to DonutChat. That is why a bot could say "I'll report back" and then
stay silent.

## Install

On the machine that runs `hermes gateway`:

```bash
hermes plugins install DonutChat/hermes-donut --enable
```

Then add the bot token to `~/.hermes/.env`, either with `hermes gateway setup` or by hand:

```bash
DONUT_BOT_TOKEN=donutbot_…
DONUT_ALLOWED_USERS=123456789      # your DonutChat user ID; comma-separate several
DONUT_HOME_CHANNEL=987654321       # optional: chat for cron results and notices
```

Restart the gateway:

```bash
hermes gateway restart
```

The plugin needs no extra packages: `httpx` and `websockets` ship with Hermes.

If the same bot is still served by `donut_hermes.py` or `hermes-bridge`, stop that first. DonutChat keeps one
event stream per bot, and two clients keep disconnecting each other.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `DONUT_BOT_TOKEN` | required | Bot token from DonutChat → Settings → My bots |
| `DONUT_ALLOWED_USERS` | none | DonutChat user IDs allowed to talk to Hermes |
| `DONUT_ALLOW_ALL_USERS` | `false` | Answer everyone the bot's trigger mode lets through |
| `DONUT_HOME_CHANNEL` | none | Chat ID for cron delivery; also settable with `/sethome` |
| `DONUT_API_URL` | `https://api.donutchat.com/bots/v1` | Bot API base |
| `DONUT_STREAM_URL` | derived from `DONUT_API_URL` | Event stream URL |

Which messages reach Hermes is decided in DonutChat by the bot's trigger mode, for example mentions only.
Hermes then applies its own allowlist on top.

### Tool progress

By default, Hermes shows what it is doing ("💻 Running …") in one thread reply that it keeps updating while
it works. To turn this off for DonutChat, add the following to `~/.hermes/config.yaml`:

```yaml
display:
  platforms:
    donut:
      tool_progress: "off"   # or new, all, verbose
```

To switch it from the chat instead, set `display.tool_progress_command: true`. `/verbose` then cycles
through off, new, all and verbose, and saves the choice for DonutChat.

## How it works

- **Inbound:** the plugin reads `message.new` and `thread.reply.new` from `wss://…/bots/v1/stream`. It
  de-duplicates by `event_id` and resumes with `last_event_id` after a reconnect.
- **Threads:** a top-level message starts a thread under itself. Every thread is its own Hermes session.
  The thread's root message is passed to Hermes once, as reply context, and linked messages are passed as
  channel context.
- **Outbound:** replies are created with `POST /thread-replies`. Streamed answers are edits through
  `POST /thread-replies/stream`. A message with no thread, such as a cron result in the home chat, uses
  `POST /messages`. Answers are split to stay under the 80,000-byte limit.
- **Attachments:** files are fetched through the authenticated attachment endpoint, without following
  redirects, and handed to Hermes as images or documents.

## Files from Hermes

Hermes can attach images (PNG, JPEG, GIF, WebP), PDFs and UTF-8 text files of up to 10 MiB to its replies,
for example with `MEDIA:/path/to/screenshot.png`. DonutChat accepts them only after the bot's owner turns
on **Allow file uploads** in the bot's settings in the iPhone, Android or desktop app. Until then, and for
other file types, Hermes posts a short notice instead of the file.

## Limits

- Files attach only to thread replies. Every agent reply is in a thread, so this matters only for cron
  results posted to the home chat, which arrive as text.
- Top-level bot messages can't be edited, so streaming only happens inside threads.

## Development

The tests run against a real Hermes checkout:

```bash
git clone https://github.com/NousResearch/hermes-agent && cd hermes-agent
UV_PROJECT_ENVIRONMENT=../venv uv sync --frozen && cd ..
venv/bin/pip install pytest pytest-asyncio   # or: VIRTUAL_ENV=venv uv pip install pytest pytest-asyncio
cd hermes-donut/tests && ../../venv/bin/python -m pytest
```

`tests/e2e_gateway.py` runs a real `hermes gateway run` with this plugin, a fake DonutChat server and a
scripted model. It checks that a background job's result reaches the thread after the turn ended:

```bash
python -I tests/e2e_gateway.py "$(which hermes)" "file://$PWD"
```

## License

MIT
