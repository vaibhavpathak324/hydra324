# HYDRA 324

Telegram **control bot** + **account session**.

You drive everything from a bot with **inline buttons** (not commands). The bot is the remote. Join requests, DMs, drafts, and posts run on a user session.

## What it does

- **Control bot** — every action is a button. Persistent **Open panel** key. First private chat becomes the owner; everyone else is ignored.
- **Session login** — phone code, 2FA, or a session string, from the bot or the web panel.
- **Joined chats** — groups, supergroups, channels, with pagination and filter.
- **Pending join requests** — listed only. Never approved or declined.
- **Same DM** — one message to every selected requester.
- **Write drafts, don’t send** — fills each requester DM as a Telegram draft.
- **Send all drafts** — one tap. Drafts/sends go out in MTProto containers, not a slow one-chat loop.
- **Inline-button posts** — the session posts messages that include URL buttons. Telegram does not let a user account attach keyboards directly, so HYDRA sends them *as the session* through this bot’s **inline mode** (`hide_via`). You can also post as the bot itself, or preview in the control chat.

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m hydra --bot-token "123456:ABC..."
```

Or paste the token in the web bar at `http://127.0.0.1:8000`.

Open the bot in Telegram → **Start** once → use only buttons after that.

## BotFather

1. `@BotFather` → new bot → copy the token.
2. **Bot Settings → Inline Mode → On** (needed for session posts with buttons). Placeholder: `hydra`.
3. Optional: disable groups if you only want the control chat.

API ID + hash for the **session** still come from [my.telegram.org](https://my.telegram.org). A bot token cannot read join requesters or write drafts in other people’s DMs.

## Typical path in the bot

1. **Session** → connect phone or paste a string.
2. **Chats** → Reload → Scan pending → tap a group.
3. **Requests** → tick people (all selected by default).
4. **Message** → set copy. **Buttons** → add URL rows if you are posting.
5. **Actions** → Write drafts, then Send all drafts. Or Send DMs now.
6. **Post** → preview, then post as session + buttons to the selected chat.

Telegram may still return `FLOOD_WAIT` or `privacy` on people who block DMs. Those show on the job screen and in the log.

Session files live in `data/` and are gitignored. The session string is equivalent to the account password.

## Deploy on Render

A `render.yaml` blueprint is included. Create the service from the repo, then set these env vars (Render dashboard or API):

| Var | What it is |
| --- | --- |
| `HYDRA_BOT_TOKEN` | Control-bot token from @BotFather |
| `TELEGRAM_API_ID` / `TELEGRAM_API_HASH` | From my.telegram.org — prefills the session login |
| `OWNER_ID` | Your Telegram user id — locks the control bot to you |
| `DATABASE_URL` | Supabase **session pooler** URL (port 5432, `ssl=require`) |
| `PYTHON_VERSION` | `3.12.6` |

Build: `pip install -r requirements.txt` · Start: `python -m hydra --port $PORT` · Health check: `/api/status`.

Render's free tier has an **ephemeral disk**, so HYDRA mirrors its state (session string, creds, bot token/owner) into the Supabase `hydra_state` table and restores it on every boot — the Telegram session survives restarts and redeploys with no re-login. Use the Supabase **session pooler** (`...pooler.supabase.com:5432`), not the direct `db.*` host: the direct host is IPv6-only and unreachable from most cloud hosts, including Render.

Free web services spin down after ~15 min without HTTP traffic; the control bot stops responding until the service wakes. Any request to the site wakes it again.
