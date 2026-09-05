from __future__ import annotations

import html
from typing import Any, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

CHATS_PER = 6
PEOPLE_PER = 8


def esc(s: Any) -> str:
    return html.escape("" if s is None else str(s), quote=False)


def clip(s: str, n: int = 36) -> str:
    s = (s or "").replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def B(text: str, data: Optional[str] = None, url: Optional[str] = None) -> InlineKeyboardButton:
    if url:
        return InlineKeyboardButton(clip(text, 60), url=url)
    return InlineKeyboardButton(clip(text, 60), callback_data=data or "nop")


def kb(rows: list[list[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(rows)


def nav(back: Optional[str] = None) -> list[InlineKeyboardButton]:
    row: list[InlineKeyboardButton] = []
    if back:
        row.append(B("← Back", back))
    row.append(B("Home", "go:home"))
    return row


def bar(done: int, total: int, width: int = 12) -> str:
    if total <= 0:
        return "░" * width
    fill = int(round(width * min(max(done, 0), total) / total))
    return "█" * fill + "░" * (width - fill)


def preview_msg(text: str, n: int = 140) -> str:
    t = (text or "").strip() or "— not set —"
    t = clip(t, n)
    return f"<code>{esc(t)}</code>"


def session_line(engine) -> str:
    if engine.phase == "ready" and engine.me:
        u = engine.me
        handle = f" @{esc(u['username'])}" if u.get("username") else ""
        return f"● <b>{esc(u.get('name'))}</b>{handle}"
    if engine.phase == "awaiting_code":
        return "○ waiting for login code"
    if engine.phase == "awaiting_password":
        return "○ waiting for 2FA password"
    return "○ no session"


def render(engine, ws, bot_username: str = "") -> tuple[str, InlineKeyboardMarkup]:
    screen = ws.screen
    fn = {
        "home": home,
        "session": session,
        "chats": chats,
        "reqs": reqs,
        "msg": message,
        "btns": buttons,
        "act": actions,
        "post": post,
        "log": logs,
        "set": settings,
        "wait": wait,
        "confirm": confirm,
        "job": job,
        "drafts": drafts,
    }.get(screen, home)
    return fn(engine, ws, bot_username)


def home(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    from hydra.engine import pool

    chat = ws.selected_chat
    chat_s = esc(chat["title"]) if chat else "none selected"
    n_sel = len(ws.selected_ids)
    n_ppl = len(ws.people)
    n_btn = sum(len(r) for r in ws.buttons)
    try:
        n_sessions = len(pool.summary())
    except Exception:
        n_sessions = 1
    more = f" (+{n_sessions - 1})" if n_sessions > 1 else ""
    text = (
        "<b>HYDRA 324</b>\n"
        "<i>tap buttons — don’t type commands</i>\n\n"
        f"Session     {session_line(engine)}{more}\n"
        f"Chat        {chat_s}\n"
        f"Requests    {n_ppl} loaded · {n_sel} selected\n"
        f"Drafts      {len(engine.armed)} armed\n"
        f"Message     {preview_msg(ws.message, 90)}\n"
        f"Buttons     {len(ws.buttons)} row · {n_btn} links\n"
    )
    rows = [
        [B("Session", "go:session"), B("Chats", "go:chats")],
        [B("Requests", "go:reqs"), B("Message", "go:msg")],
        [B("Buttons", "go:btns"), B("Actions", "go:act")],
        [B("Post", "go:post"), B("Log", "go:log")],
        [B("Settings", "go:set")],
    ]
    return text, kb(rows)


def session(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    from hydra.engine import pool

    sessions = pool.summary()
    if not sessions:
        text = (
            "<b>Sessions</b>\n\n"
            "No account session connected.\n"
            "The control bot is only the remote — requests and DMs run on user sessions."
        )
        rows = [
            [B("➕ Add session — phone", "sess:phone")],
            [B("➕ Add — session string", "sess:string")],
            nav(),
        ]
        return text, kb(rows)

    lines: list[str] = []
    rows: list[list[InlineKeyboardButton]] = []
    for s in sessions:
        me = s.get("me") or {}
        who = me.get("username") or me.get("name") or me.get("phone") or s["key"]
        mark = "●" if s["active"] else "○"
        extra = []
        if s.get("auto"):
            extra.append("auto")
        if s.get("armed"):
            extra.append(f"{s['armed']} drafts")
        tail = (" · " + ", ".join(extra)) if extra else ""
        lines.append(f"{mark} {esc(str(who))}{tail}")
        label = f"{mark} {str(who)}"[:60]
        rows.append([B(label, f"sess:sw:{s['key']}"), B("✕", f"sess:rm:{s['key']}")])
    text = (
        "<b>Sessions</b>\n\n"
        + "\n".join(lines)
        + "\n\n<i>Tap a session to make it active — every action runs on it. "
        "✕ removes it. All sessions stay connected and keep auto-sending.</i>"
    )
    rows.append([B("➕ Add session — phone", "sess:phone")])
    rows.append([B("➕ Add — session string", "sess:string")])
    if engine.phase == "ready":
        rows.append([B("Export active session", "sess:export")])
    rows.append(nav())
    return text, kb(rows)


def chats(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    rows_data = ws.visible_chats()
    page = ws.chat_page
    start = page * CHATS_PER
    chunk = rows_data[start : start + CHATS_PER]
    filt = f"\nFilter: <code>{esc(ws.chat_filter)}</code>" if ws.chat_filter else ""
    text = (
        "<b>Admin chats</b>\n"
        f"{len(rows_data)} chat(s) where you're admin{filt}\n"
        "Only these can list join requests."
    )
    rows: list[list[InlineKeyboardButton]] = []
    for i, c in enumerate(chunk):
        idx = start + i
        mark = "● " if ws.selected_chat and c["id"] == ws.selected_chat["id"] else ""
        pending = "" if c.get("pending") is None else f" · {c['pending']} pending"
        admin = " · admin" if c.get("admin") else ""
        rows.append([B(f"{mark}{c['title']}{pending}", f"ch:{idx}")])
        # second line as small meta isn't possible; encode in title
        _ = admin
    navrow: list[InlineKeyboardButton] = []
    if page > 0:
        navrow.append(B("◀", f"chats:p:{page - 1}"))
    navrow.append(B(f"{page + 1}/{(max(len(rows_data) - 1, 0) // CHATS_PER) + 1}", "nop"))
    if start + CHATS_PER < len(rows_data):
        navrow.append(B("▶", f"chats:p:{page + 1}"))
    rows.append(navrow)
    rows.append([B("Reload", "chats:load"), B("Scan pending", "chats:scan")])
    rows.append([B("Filter", "chats:filter")])
    rows.append(nav("go:home"))
    return text, kb(rows)


def reqs(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    if not ws.selected_chat:
        text = "<b>Join requests</b>\n\nSelect a group or channel first. Requests are never approved or declined."
        return text, kb([[B("Open chats", "go:chats")], nav()])
    people = ws.visible_people()
    page = ws.people_page
    start = page * PEOPLE_PER
    chunk = people[start : start + PEOPLE_PER]
    title = esc(ws.selected_chat["title"])
    dmed = sum(1 for p in ws.people if p.get("dm_sent"))
    text = (
        f"<b>Join requests</b>\n{title}\n"
        f"{len(ws.people)} pending · {len(ws.selected_ids)} selected"
        + (f" · {dmed} already DMed ✅" if dmed else "")
        + "\n<i>Read only — HYDRA does not accept or decline.</i>"
    )
    rows: list[list[InlineKeyboardButton]] = []
    for i, p in enumerate(chunk):
        idx = start + i
        on = "☑" if p["id"] in ws.selected_ids else "☐"
        uname = f" @{p['username']}" if p.get("username") else ""
        mark = " ✅" if p.get("dm_sent") else ""
        rows.append([B(f"{on}  {p['name']}{uname}{mark}", f"rq:{idx}")])
    navrow: list[InlineKeyboardButton] = []
    last_page = max(len(people) - 1, 0) // PEOPLE_PER
    if page > 0:
        navrow.append(B("◀", f"reqs:p:{page - 1}"))
    navrow.append(B(f"{page + 1}/{last_page + 1}", "nop"))
    if start + PEOPLE_PER < len(people):
        navrow.append(B("▶", f"reqs:p:{page + 1}"))
    rows.append(navrow)
    rows.append([B("Select all", "reqs:all"), B("Unsent only", "reqs:unsent"), B("Select none", "reqs:none")])
    rows.append([B("Reload", "reqs:load"), B("Filter", "reqs:filter")])
    rows.append(nav("go:chats"))
    return text, kb(rows)


def message(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    body = ws.message.strip() if ws.message else ""
    shown = esc(body) if body else "<i>empty</i>"
    text = (
        "<b>Message</b>\n\n"
        "This is the copy used for DMs, drafts, and posts.\n\n"
        f"{shown}"
    )
    rows = [
        [B("Set message", "msg:set")],
        [B("Clear", "msg:clear")],
        [B("Edit buttons", "go:btns")],
        nav(),
    ]
    return text, kb(rows)


def buttons(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    if not ws.buttons:
        layout = "<i>no buttons yet</i>"
    else:
        lines = []
        for i, row in enumerate(ws.buttons, 1):
            bits = " · ".join(f"{esc(b['text'])}" for b in row)
            lines.append(f"{i}. {bits}")
        layout = "\n".join(lines)
    text = (
        "<b>Inline buttons</b>\n\n"
        "URL buttons attached to posts the session sends "
        "(via this bot’s inline mode) or that this bot posts itself.\n\n"
        f"{layout}"
    )
    rows = [
        [B("Add URL button", "btn:add")],
        [B("New row", "btn:row"), B("Remove last", "btn:del")],
        [B("Clear all", "btn:clr")],
        [B("Preview", "post:preview")],
        nav(),
    ]
    return text, kb(rows)


def drafts(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    armed = engine.armed
    if armed:
        unique: list[str] = []
        for t in armed:
            if t.message not in unique:
                unique.append(t.message)
        preview = esc(unique[0][:700])
        extra = f"\n\n<i>(+ {len(unique) - 1} different message(s) across drafts)</i>" if len(unique) > 1 else ""
        text = (
            "<b>Draft status</b>\n\n"
            f"Drafts written, waiting to send: <b>{len(armed)}</b>\n\n"
            f"<b>Draft message:</b>\n{preview}{extra}\n\n"
            "<i>Write drafts resumes — people who already have a draft are skipped. "
            "It starts over only after Send all drafts or Clear drafts.</i>"
        )
    else:
        text = (
            "<b>Draft status</b>\n\n"
            "No drafts written.\n\n"
            "Actions → Write drafts — don't send."
        )
    rows = [
        [B("Refresh", "go:drafts")],
        [B("Send all drafts", "act:fire"), B("Clear drafts", "act:disarm")],
        nav(),
    ]
    return text, kb(rows)


def actions(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    n = len(ws.selected_people())
    auto = engine.auto_status()
    if auto["on"]:
        auto_line = (
            f"on · {auto['remaining']} left · next pass in ~{max(1, auto['next_in'])}s "
            f"(every {auto['interval']}s)"
        )
    else:
        auto_line = f"off (every {auto['interval']}s when on)"
    draft_msg = esc(ws.message.strip()[:80]) if ws.message.strip() else "<i>unset</i>"
    text = (
        "<b>Actions</b>\n\n"
        f"Selected requesters: <b>{n}</b>\n"
        f"Armed drafts: <b>{len(engine.armed)}</b>\n"
        f"Broadcast list: <b>{len(engine.dmdir)}</b> known DMs\n"
        f"Auto-send: <b>{auto_line}</b>\n\n"
        f"Message copy: {draft_msg}\n\n"
        "Write drafts fills each DM compose box and does <b>not</b> send.\n"
        "Send all drafts is the one-click release.\n"
        "Auto-send keeps DMing the unsent selection automatically until done.\n"
        "Broadcast re-DMs everyone on the broadcast list."
    )
    rows = [
        [B("Write drafts — don’t send", "act:arm")],
        [B("Send all drafts", "act:fire")],
        [B("Send DMs now", "act:send")],
        [B("Send DMs with buttons", "act:inlinedm")],
        [B("Clear drafts", "act:disarm")],
        [B("📄 Draft status", "go:drafts")],
        [B("📢 Broadcast", "act:bcast")],
        [B(
            f"Auto-send: {'ON — tap to stop' if auto['on'] else 'OFF — tap to start'}",
            "act:auto",
        )],
        [B("Auto interval", "act:autoint")],
        nav(),
    ]
    return text, kb(rows)


def post(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    target = esc(ws.selected_chat["title"]) if ws.selected_chat else "none — pick a chat first"
    bot = f"@{esc(bot_username)}" if bot_username else "this bot"
    n_btn = sum(len(r) for r in ws.buttons)
    text = (
        "<b>Post</b>\n\n"
        f"Target    {target}\n"
        f"Buttons   {n_btn}\n"
        f"Copy      {preview_msg(ws.message, 80)}\n\n"
        "A user session cannot attach inline keyboards by itself. "
        f"HYDRA posts button messages <b>as the session</b> through {bot} "
        "inline mode (<code>hide_via</code>), so it still looks like your account.\n\n"
        "Turn on Inline Mode for this bot in @BotFather if you have not."
    )
    rows = [
        [B("Preview in this chat", "post:preview")],
        [B("Post as session + buttons", "act:postinline")],
        [B("Post as session, text only", "act:postsession")],
        [B("Post as this bot + buttons", "act:postbot")],
        [B("Pick a chat", "go:chats")],
        nav(),
    ]
    return text, kb(rows)


def logs(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    items = list(engine.logs)[-12:]
    if not items:
        body = "<i>nothing yet</i>"
    else:
        lines = []
        for e in reversed(items):
            t = (e.get("ts") or "")[11:19]
            lines.append(f"<code>{esc(t)}</code> {esc(e.get('text'))}")
        body = "\n".join(lines)
    text = f"<b>Log</b>\n\n{body}"
    return text, kb([[B("Refresh", "go:log")], nav()])


def settings(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    from hydra.engine import pool

    bot = f"@{esc(bot_username)}" if bot_username else "—"
    n_sessions = len(pool.summary())
    text = (
        "<b>Settings</b>\n\n"
        f"Control bot    {bot}\n"
        f"Session        {session_line(engine)}\n"
        f"Sessions       {n_sessions}\n\n"
        "This bot only answers the first Telegram account that opens it "
        "(the owner). Everyone else is ignored.\n\n"
        "Inline mode: @BotFather → your bot → Bot Settings → Inline Mode → On.\n"
        "Placeholder can be <code>hydra</code>."
    )
    rows = [
        [B("Refresh panel", "go:home")],
        [B("Export session", "sess:export")],
        [B(f"Clear DM history ({len(engine.sent_ids)})", "act:clrsent")],
        nav(),
    ]
    return text, kb(rows)


def wait(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    prompts = {
        "api_id": ("Connect session", "1 / 4 · API ID", "Send the API ID as your next message (numbers only)."),
        "api_hash": ("Connect session", "2 / 4 · API hash", "Send the API hash from my.telegram.org."),
        "phone": ("Connect session", "3 / 4 · Phone", "Send the phone with country code.\nExample: +447700900123"),
        "code": ("Connect session", "4 / 4 · Login code", "Send the login code Telegram just delivered."),
        "password": ("Connect session", "Two-step password", "Send the 2FA password."),
        "s_api_id": ("Session string", "1 / 3 · API ID", "Send the API ID."),
        "s_api_hash": ("Session string", "2 / 3 · API hash", "Send the API hash."),
        "session_string": ("Session string", "3 / 3 · String", "Paste the session string as your next message."),
        "message": ("Message", "Send copy", "Send the DM / post text as your next message."),
        "btn_label": ("Add button", "1 / 2 · Label", "Send the button label."),
        "btn_url": ("Add button", "2 / 2 · URL", "Send the URL, including https://"),
        "chat_filter": ("Filter chats", "", "Send text to match titles. Send a single dash (-) to clear."),
        "people_filter": ("Filter people", "", "Send text to match names. Send a single dash (-) to clear."),
        "autoint": ("Auto-send interval", "", "Send the seconds between automatic passes (3–120)."),
    }
    title, step, hint = prompts.get(ws.waiting or "", ("HYDRA", "", "Send your next message."))
    step_l = f"\n{esc(step)}\n" if step else "\n"
    text = f"<b>{esc(title)}</b>{step_l}\n{esc(hint)}"
    return text, kb([[B("Cancel", "wait:cancel")], nav()])


def confirm(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    action = ws.pending or ""
    n = len(ws.selected_people())
    copies = {
        "arm": (
            "Write unsent drafts",
            f"Place the message in <b>{n}</b> requester DMs as drafts. Nothing is sent.",
            "do:arm",
        ),
        "fire": (
            "Send all drafts",
            f"Release <b>{len(engine.armed)}</b> armed drafts now.",
            "do:fire",
        ),
        "clrsent": (
            "Clear DM history",
            f"Forget the <b>{len(engine.sent_ids)}</b> stored already-DMed records. "
            "Those people become selectable for DMs again.",
            "do:clrsent",
        ),
        "bcast": (
            "📢 Broadcast",
            f"Send the current message to <b>all {len(engine.dmdir)}</b> known DM "
            "recipients — everyone this account has ever DMed. They get it even "
            "if they were DMed before.",
            "do:bcast",
        ),
        "sesrm": (
            "Remove session",
            f"Log out and remove <code>{esc(str(ws.login.get('_rmkey', '')))}</code>? "
            "Its drafts and DM history for that account are deleted.",
            "do:sesrm",
        ),
        "send": (
            "Send DMs now",
            f"Send the message immediately to <b>{n}</b> requesters.",
            "do:send",
        ),
        "auto": (
            "Auto-send ON",
            f"Keep DMing the unsent selection automatically — one pass every "
            f"<b>{int((engine.auto or {}).get('interval', 30) or 30)}s</b> until everyone "
            "is DMed. Nobody gets two DMs.",
            "do:auto",
        ),
        "inlinedm": (
            "Send DMs with buttons",
            f"The session sends the button message to <b>{n}</b> requesters via inline mode.",
            "do:inlinedm",
        ),
        "disarm": (
            "Clear drafts",
            f"Erase {len(engine.armed)} armed drafts from DMs.",
            "do:disarm",
        ),
        "logout": (
            "Log out session",
            "Disconnect the user session. The control bot stays up.",
            "do:logout",
        ),
        "postinline": (
            "Post as session + buttons",
            "The account posts to the selected chat with inline buttons (via this bot, hide via).",
            "do:postinline",
        ),
        "postsession": (
            "Post as session",
            "The account posts text only to the selected chat.",
            "do:postsession",
        ),
        "postbot": (
            "Post as this bot + buttons",
            "The control bot posts to the selected chat. It must be a member / admin there.",
            "do:postbot",
        ),
    }
    title, body, yes = copies.get(action, ("Confirm", "Do this?", "go:home"))
    text = f"<b>{esc(title)}</b>\n\n{body}"
    rows = [[B("Confirm", yes)], [B("Cancel", "go:home")]]
    return text, kb(rows)


def job(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    j = engine.job
    if not j:
        return "<b>Job</b>\n\nNo job running.", kb([nav()])
    pct = int(round(100 * j.done / j.total)) if j.total else 0
    skip_s = f" · skip {j.skipped}" if j.skipped else ""
    text = (
        f"<b>{esc(j.kind)}</b>\n"
        f"<code>{bar(j.done, j.total)}</code>  {pct}%\n\n"
        f"{j.done}/{j.total} · ok {j.ok} · fail {j.fail}{skip_s}\n"
        f"{esc(j.detail or j.status)}"
    )
    if j.errors:
        bits = ", ".join(f"{e.get('user_id')}:{e.get('error')}" for e in j.errors[-4:])
        text += f"\n\n<code>{esc(bits)}</code>"
    rows = [[B("Refresh", "go:job")]]
    if j.status == "running":
        rows.append([B("⏹ Cancel job", "job:cancel")])
    else:
        rows.append([B("Home", "go:home"), B("Log", "go:log")])
    return text, kb(rows)
