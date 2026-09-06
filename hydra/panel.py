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
    if engine.phase == "dead_key":
        return "⚠️ saved — needs re-login"
    if engine.phase == "waiting":
        return "🔄 reconnecting…"
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
        "auto": auto_progress,
        "groups": groups,
        "postedit": postedit,
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
        f"DMs sent    {len(engine.sent_ids)}\n"
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
            [B("📤 Send login post to a chat", "sess:sendpost")],
            [B("✍️ Edit login post (text · image · button)", "go:postedit")],
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
        st = s.get("phase")
        if st == "dead_key":
            extra.append("⚠️ needs re-login")
        elif st == "waiting":
            extra.append("🔄 reconnecting")
        elif st not in ("ready", "awaiting_code", "awaiting_password"):
            extra.append("logged out")
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
    rows.append([B("✍️ Edit login post (text · image · button)", "go:postedit")])
    rows.append([B("📨 Login post — preview here", "sess:loginpost")])
    rows.append([B("📤 Send login post to a chat", "sess:sendpost")])
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


def auto_progress(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    st = engine.auto_status()
    if not st["on"]:
        text = (
            "<b>Auto-send</b>\n\n"
            "Off.\n\n"
            "To start: pick a chat in Chats, select people in Requests, "
            "set the Message, then Actions → Auto-send."
        )
        rows = [
            [B("Open Actions", "go:act")],
            nav(),
        ]
        return text, kb(rows)
    a = engine.auto or {}
    j = engine.job
    prog = ""
    if j and j.status == "running" and j.kind == "send":
        pct = int(round(100 * j.done / j.total)) if j.total else 0
        prog = (
            f"\n\n<b>Pass in progress</b>\n<code>{bar(j.done, j.total)}</code>  {pct}%\n"
            f"{j.done}/{j.total} · ok {j.ok} · fail {j.fail}"
        )
    elif a.get("last_result"):
        prog = f"\n\nLast pass: <b>{esc(str(a['last_result']))}</b>"
    text = (
        "<b>Auto DMs · ON</b>\n"
        f"Chat: {esc(str(a.get('title') or a.get('chat_id') or '—'))}\n\n"
        f"Sent so far: <b>{int(a.get('sent_total', 0) or 0)}</b>\n"
        f"Still unsent: <b>{st['remaining']}</b>\n"
        f"Passes done: <b>{int(a.get('passes', 0) or 0)}</b>\n"
        "Mode: continuous — no pauses"
        f"{prog}\n\n"
        "<i>Sends back-to-back until everyone is DMed or you stop it. "
        "Nobody gets two DMs.</i>"
    )
    rows = [
        [B("⏹ Stop auto-send", "auto:stop")],
        [B("⚡ Send a pass now", "auto:pass")],
        [B("Refresh", "go:auto")],
        nav(),
    ]
    return text, kb(rows)


def actions(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    n = len(ws.selected_people())
    auto = engine.auto_status()
    chat = ws.selected_chat.get("title") if ws.selected_chat else None
    msg = esc(ws.message.strip()[:80]) if ws.message.strip() else "<i>unset</i>"
    if auto["on"]:
        status = (
            f"🟢 Running · {int((engine.auto or {}).get('sent_total', 0) or 0)} sent "
            f"· {auto['remaining']} left"
        )
    else:
        status = "⚪ Not running"
    text = (
        "<b>Actions</b>\n\n"
        f"Message:  {msg}\n"
        f"Selected: {n} people" + (f" in {esc(str(chat))}" if chat else "") + "\n"
        f"{status}\n\n"
        "<i>Start DMs sends your message to everyone selected and keeps going "
        "automatically until all are DMed. Nobody gets two DMs.</i>"
    )
    main = (
        [B("⏹ Stop DMs", "act:autodmstop")]
        if auto["on"]
        else [B("🚀 Start DMs", "act:startdm")]
    )
    rows = [
        main,
        [B("📊 Progress", "go:auto")],
        [B("👥 Groups · join & broadcast", "go:groups")],
        [B("📢 Broadcast to all known DMs", "act:bcast")],
        nav(),
    ]
    return text, kb(rows)


def postedit(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    """Login post builder: text, image, button label, preview, delivery."""
    cfg = getattr(ws, "postcfg", None) or {"text": None, "photo": None, "button": None}
    text = cfg.get("text")
    button = cfg.get("button") or "\U0001F4F2 Connect this account"
    body = text if text else "📲 Connect this account to HYDRA\n(default text)"
    lines = [
        "<b>Login post builder</b>",
        "",
        f"<b>Text:</b>\n{preview_msg(body, 260)}",
        f"<b>Image:</b> {'✅ set' if cfg.get('photo') else '— none (text-only post)'}",
        f"<b>Button:</b> {esc(str(button))}  <i>(triggers share-contact → you finish the login)</i>",
        "",
        "<i>Delivered to a DM, the post carries your button as a native share-contact "
        "button — tapping it pops Telegram's own \u201cShare phone number?\u201d confirmation. "
        "In groups it becomes an inline Connect button. Forwarded copies lose "
        "buttons, so always deliver via 'Send to a chat'.</i>",
    ]
    rows = [
        [B("📝 Set text", "post:txt"), B("🖼 Set image", "post:pic")],
        [B("🔘 Button text", "post:btn")],
        [B("👁 Preview here", "post:preview"), B("♻️ Reset", "post:reset")],
        [B("📤 Send to a chat", "post:send")],
        nav("session"),
    ]
    return "\n".join(lines), kb(rows)


def groups(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    joins = engine.joins or []
    if joins:
        head = "<b>Groups</b>\n\n" + "\n".join(
            f"• {esc(str(j.get('title') or j.get('id')))}" for j in joins
        )
        head += "\n\n<i>Broadcast to groups sends the current message to all of them as the session.</i>"
    else:
        head = (
            "<b>Groups</b>\n\n"
            "No groups joined yet.\n\n"
            "Join a group with its @username or t.me invite link — "
            "then broadcast to all of them with one tap."
        )
    rows: list[list[InlineKeyboardButton]] = []
    for j in joins:
        rows.append(
            [
                B(str(j.get("title") or j.get("id"))[:56], "nop"),
                B("✕", f"grm:{j['id']}"),
            ]
        )
    rows.append([B("➕ Join group", "act:joingroup")])
    if joins:
        rows.append([B("📣 Broadcast to groups", "act:gbcast")])
    rows.append(nav())
    return head, kb(rows)


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
        "joingroup": ("Join group", "", "Send the group's @username or t.me link (t.me/+… invites work too)."),
        "loginpost_text": ("Craft login post", "", "Send the text for your login post. Send a single dash (-) to reset to the default."),
        "posttext": ("Login post · text", "", "Send the post text (emoji, multiple lines — anything). Send a dash (-) to go back to the default text."),
        "postpic": ("Login post · image", "", "Send the photo to use in the post. Send 'no image' as text to remove the current one."),
        "postbtn": ("Login post · button", "", f"Send the button label (max 40 chars). Current: {'📲 Connect this account'}"),
        "sendpostto": ("Send login post", "", "Where should the login post go? Send a @username, a t.me/… link, or a numeric chat id. The bot must be able to message that chat."),
    }
    title, step, hint = prompts.get(ws.waiting or "", ("HYDRA", "", "Send your next message."))
    step_l = f"\n{esc(step)}\n" if step else "\n"
    text = f"<b>{esc(title)}</b>{step_l}\n{esc(hint)}"
    return text, kb([[B("Cancel", "wait:cancel")], nav()])


def confirm(engine, ws, bot_username: str) -> tuple[str, InlineKeyboardMarkup]:
    action = ws.pending or ""
    n = len(ws.selected_people())
    copies = {
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
        "gbcast": (
            "📣 Broadcast to groups",
            f"Send the current message to <b>all {len(engine.joins)}</b> joined "
            "groups/channels, as the session.",
            "do:gbcast",
        ),
        "sesrm": (
            "Remove session",
            f"Log out and remove <code>{esc(str(ws.login.get('_rmkey', '')))}</code>? "
            "Its drafts and DM history for that account are deleted.",
            "do:sesrm",
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
