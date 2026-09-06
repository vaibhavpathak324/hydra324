from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    KeyboardButton,
    MenuButtonCommands,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    MessageHandler,
    filters,
)

from hydra import store
from hydra.engine import DATA, engine, pool
from hydra.panel import esc, render

log = logging.getLogger("hydra.bot")
BOT_PATH = DATA / "bot.json"
URL_RE = re.compile(r"^https?://", re.I)

OPEN_PANEL = "Open panel"
DEFAULT_LOGIN_POST = (
    "\U0001F4F2 <b>Connect this account to HYDRA</b>\n\n"
    "Tap the button below and share your phone number \u2014 "
    "that's all you do on this side."
)


@dataclass
class Workspace:
    screen: str = "home"
    waiting: Optional[str] = None
    pending: Optional[str] = None
    message: str = ""
    buttons: list[list[dict[str, str]]] = field(default_factory=list)
    chats: list[dict[str, Any]] = field(default_factory=list)
    people: list[dict[str, Any]] = field(default_factory=list)
    selected_chat: Optional[dict[str, Any]] = None
    selected_ids: set[int] = field(default_factory=set)
    chat_page: int = 0
    people_page: int = 0
    chat_filter: str = ""
    people_filter: str = ""
    login: dict[str, str] = field(default_factory=dict)
    tmp_label: str = ""
    panel_chat_id: Optional[int] = None
    panel_msg_id: Optional[int] = None
    return_screen: str = "home"

    def visible_chats(self) -> list[dict[str, Any]]:
        q = (self.chat_filter or "").lower().strip()
        if not q:
            return self.chats
        return [c for c in self.chats if q in (c.get("title") or "").lower()]

    def visible_people(self) -> list[dict[str, Any]]:
        q = (self.people_filter or "").lower().strip()
        rows = self.people
        if q:
            rows = [
                p
                for p in rows
                if q in f"{p.get('name')} {p.get('username') or ''} {p.get('about') or ''}".lower()
            ]
        return rows

    def selected_people(self) -> list[dict[str, Any]]:
        return [p for p in self.people if p["id"] in self.selected_ids]


def _valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def user_markup(rows: list[list[dict[str, str]]]) -> Optional[InlineKeyboardMarkup]:
    if not rows:
        return None
    from telegram import InlineKeyboardButton

    out = []
    for row in rows:
        out.append(
            [
                InlineKeyboardButton(b.get("text") or "Open", url=b.get("url") or "https://t.me")
                for b in row
            ]
        )
    return InlineKeyboardMarkup(out) if out else None


class BotController:
    def __init__(self) -> None:
        self.application: Optional[Application] = None
        self.token: Optional[str] = None
        self.username: Optional[str] = None
        self.bot_id: Optional[int] = None
        self.owner_id: Optional[int] = None
        self.ws = Workspace()
        self.running = False
        self.busy = False
        self._pump_task: Optional[asyncio.Task] = None
        self._queue = None
        self.inline_store: dict[str, dict[str, Any]] = {}
        # Engine being logged in by the wizard (multi-session support).
        self._login_eng = None
        # Digit-pad buffer for button-only login code entry.
        self._pad = ""
        # 2FA keypad buffer + mode (abc / ABC / 123 / sym).
        self._pw = ""
        self._pw_mode = "abc"
        # Pending logins by sharer uid — accounts HYDRA already knows may
        # finish their OWN re-login with keypads in their own chat.
        self._logins: dict[int, Any] = {}
        # Bot poller diagnostics: see /api/status -> bot.
        self._diag: dict[str, Any] = {"last_update": None, "last_error": None, "last_error_ts": None}
        # Local-keypad pairing for NEW accounts: a short-lived owner-generated
        # code that, when typed in the new account's chat, unlocks the code
        # keypad there. Proof the person at that keyboard holds the panel.
        self._pair: Optional[dict] = None

    # ── workspace persistence (survives restarts) ───────────
    def _ws_persist_core(self) -> None:
        store.push_soon(
            "ws",
            {
                "message": self.ws.message,
                "buttons": self.ws.buttons,
                "chats": self.ws.chats,
                "selected_chat": self.ws.selected_chat,
                "chat_filter": self.ws.chat_filter,
                "people_filter": self.ws.people_filter,
            },
        )

    async def _ws_persist_core_now(self) -> None:
        """Synchronous variant for critical saves — survives instant restarts."""
        await store.set(
            "ws",
            {
                "message": self.ws.message,
                "buttons": self.ws.buttons,
                "chats": self.ws.chats,
                "selected_chat": self.ws.selected_chat,
                "chat_filter": self.ws.chat_filter,
                "people_filter": self.ws.people_filter,
            },
        )

    async def _finish_login(self, le: Any) -> str:
        """Complete the wizard: adopt the new session and make it active."""
        eng = le if le is not None else self._login_eng
        if eng is not None:
            await pool.adopt(eng)
            if self._login_eng is eng:
                self._login_eng = None
            self._logins = {uid: e for uid, e in self._logins.items() if e is not eng}
            msg = "Session added — now active."
        else:
            msg = "Session live."
        self.ws.chats = []
        self.ws.people = []
        self.ws.selected_chat = None
        self.ws.selected_ids = set()
        store.push_soon("ws_people", None)
        store.push_soon("ws_sel", None)
        await self._ws_persist_core_now()
        self.ws.waiting = None
        self.ws.screen = "home"
        return msg

    def _ws_persist_people(self) -> None:
        store.push_soon("ws_people", self.ws.people)

    def _ws_persist_sel(self) -> None:
        store.push_soon("ws_sel", sorted(self.ws.selected_ids))

    async def _ws_restore(self) -> None:
        try:
            core = await store.get("ws") or {}
            if core.get("message"):
                self.ws.message = core["message"]
            if core.get("buttons"):
                self.ws.buttons = core["buttons"]
            self.ws.chats = core.get("chats") or []
            self.ws.selected_chat = core.get("selected_chat")
            self.ws.chat_filter = core.get("chat_filter") or ""
            self.ws.people_filter = core.get("people_filter") or ""
            self.ws.people = await store.get("ws_people") or []
            ids = await store.get("ws_sel") or []
            valid = {p.get("id") for p in self.ws.people}
            self.ws.selected_ids = {i for i in ids if i in valid}
            if self.ws.message or self.ws.people:
                engine.note("ok", "Workspace restored — message, chats, requests, selection.")
        except Exception:
            pass

    # ── persistence ─────────────────────────────────────────
    def _load(self) -> dict[str, Any]:
        if BOT_PATH.exists():
            try:
                return json.loads(BOT_PATH.read_text())
            except Exception:
                return {}
        return {}

    def _save(self) -> None:
        DATA.mkdir(parents=True, exist_ok=True)
        raw = self._load()
        if self.token:
            raw["token"] = self.token
        if self.owner_id:
            raw["owner_id"] = self.owner_id
        if self.username:
            raw["username"] = self.username
        BOT_PATH.write_text(json.dumps(raw))
        # Mirror to the state store so the token/owner survive restarts.
        store.push_soon("bot", raw)

    def status(self) -> dict[str, Any]:
        import time as _t

        out = {
            "running": self.running,
            "username": self.username,
            "owner_id": self.owner_id,
            "has_token": bool(self.token or self._load().get("token")),
        }
        d = self._diag
        age = (int(_t.time() - d["last_update"]) if d.get("last_update") else None)
        out["updates_idle_s"] = age
        out["last_error"] = d.get("last_error")
        out["poller_alive"] = bool(
            self.application and self.application.updater and self.application.updater.running
        )
        return out

    def _auth(self, uid: int) -> bool:
        if self.owner_id is None:
            self.owner_id = uid
            self._save()
            engine.note("ok", f"Control bot owner set to {uid}.")
            return True
        return uid == self.owner_id

    def _allowed_inline(self, uid: int) -> bool:
        if uid == self.owner_id:
            return True
        if engine.me and uid == engine.me.get("id"):
            return True
        return False

    # ── lifecycle ───────────────────────────────────────────
    async def start(self, token: Optional[str] = None) -> dict[str, Any]:
        if not BOT_PATH.exists():
            # Ephemeral filesystem: restore token/owner from the state store.
            await store.pull_to_file("bot", BOT_PATH)
        token = (token or os.environ.get("HYDRA_BOT_TOKEN") or os.environ.get("BOT_TOKEN") or self._load().get("token") or "").strip()
        if not token or ":" not in token:
            return {"running": False, "detail": "No bot token yet."}
        if self.running and token == self.token:
            return self.status()
        if self.running:
            await self.stop()

        stored = self._load()
        if not stored.get("owner_id"):
            env_owner = (os.environ.get("OWNER_ID") or os.environ.get("HYDRA_OWNER_ID") or "").strip()
            if env_owner.isdigit():
                stored["owner_id"] = int(env_owner)
        self.token = token
        self.owner_id = stored.get("owner_id")
        if not self.ws.message and not self.ws.people:
            await self._ws_restore()
        self.application = (
            Application.builder()
            .token(token)
            .concurrent_updates(True)
            .build()
        )
        self._register(self.application)
        try:
            await self.application.initialize()
            await self.application.start()
            assert self.application.updater
            await self.application.updater.start_polling(
                allowed_updates=["message", "callback_query", "inline_query"],
            )
            me = await self.application.bot.get_me()
        except Exception:
            await self.stop()
            raise
        self.username = me.username
        self.bot_id = me.id
        self.running = True
        self._save()
        try:
            await self.application.bot.set_my_commands(
                [BotCommand("start", "Open panel")]
            )
            await self.application.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
        except TelegramError:
            pass
        self._pump_task = asyncio.create_task(self._pump(), name="hydra-bot-pump")
        engine.note("ok", f"Control bot live as @{self.username}.")
        return self.status()

    async def stop(self) -> None:
        self.running = False
        if self._pump_task:
            self._pump_task.cancel()
            self._pump_task = None
        if self._queue is not None:
            engine.unlisten(self._queue)
            self._queue = None
        if self.application:
            try:
                if self.application.updater and self.application.updater.running:
                    await self.application.updater.stop()
                if self.application.running:
                    await self.application.stop()
                await self.application.shutdown()
            except Exception as exc:
                log.warning("bot stop: %s", exc)
        self.application = None

    async def _on_app_error(self, update, context) -> None:
        err = context.error
        txt = f"{type(err).__name__}: {str(err)[:160]}"
        import time as _t

        self._diag["last_error"] = txt
        self._diag["last_error_ts"] = _t.time()
        log.warning("bot error: %s", txt)

    def _stamp(self) -> None:
        import time as _t

        self._diag["last_update"] = _t.time()

    def _register(self, app: Application) -> None:
        app.add_error_handler(self._on_app_error)
        app.add_handler(CommandHandler("start", self.on_start))
        app.add_handler(CallbackQueryHandler(self.on_callback))
        app.add_handler(InlineQueryHandler(self.on_inline))
        app.add_handler(
            MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, self.on_text)
        )
        app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.CONTACT, self.on_contact))
        app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.PHOTO, self.on_photo))

    async def _pump(self) -> None:
        q = engine.listen()
        self._queue = q
        try:
            while self.running:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if msg.get("event") in ("job", "status", "log") and self.ws.screen == "job":
                    try:
                        await self.paint()
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass
        finally:
            engine.unlisten(q)

    # ── paint ───────────────────────────────────────────────
    def _reply_kb(self) -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            [[KeyboardButton(OPEN_PANEL)]],
            resize_keyboard=True,
            is_persistent=True,
        )

    async def paint(self, chat_id: Optional[int] = None, new: bool = False) -> None:
        if not self.application:
            return
        text, markup = render(engine, self.ws, self.username or "")
        bot = self.application.bot
        cid = chat_id or self.ws.panel_chat_id
        if cid is None:
            return
        if new or not self.ws.panel_msg_id:
            sent = await bot.send_message(
                cid,
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
                disable_web_page_preview=True,
            )
            self.ws.panel_chat_id = sent.chat_id
            self.ws.panel_msg_id = sent.message_id
            return
        try:
            await bot.edit_message_text(
                text,
                chat_id=cid,
                message_id=self.ws.panel_msg_id,
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
                disable_web_page_preview=True,
            )
        except BadRequest as exc:
            if "not modified" in str(exc).lower():
                return
            sent = await bot.send_message(
                cid,
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
                disable_web_page_preview=True,
            )
            self.ws.panel_chat_id = sent.chat_id
            self.ws.panel_msg_id = sent.message_id

    # ── handlers ────────────────────────────────────────────
    # ── button-only login keypad ─────────────────────────────
    def _pad_kb(self) -> InlineKeyboardMarkup:
        rows = []
        for row in (["1", "2", "3"], ["4", "5", "6"], ["7", "8", "9"]):
            rows.append([InlineKeyboardButton(d, callback_data=f"pad:{d}") for d in row])
        rows.append(
            [
                InlineKeyboardButton("\u232B", callback_data="pad:del"),
                InlineKeyboardButton("0", callback_data="pad:0"),
                InlineKeyboardButton("\u2705 Submit", callback_data="pad:ok"),
            ]
        )
        rows.append([InlineKeyboardButton("\u2328\uFE0F Type instead", callback_data="pad:text")])
        return InlineKeyboardMarkup(rows)

    def _pad_text(self) -> str:
        dots = " ".join("\u2022" for _ in self._pad) if self._pad else "\u2014"
        return (
            "\U0001F511 <b>Login code</b>\n\n"
            f"<b>{dots}</b>\n\n"
            "Tap the digits, then \u2705 Submit.\n"
            "<i>Read the code inside the OTHER account's Telegram app.</i>"
        )

    async def _send_pad(self, chat_id: int) -> None:
        if not (self.application and chat_id):
            return
        self._pad = ""
        try:
            await self.application.bot.send_message(
                chat_id, self._pad_text(), parse_mode=ParseMode.HTML, reply_markup=self._pad_kb()
            )
        except TelegramError:
            pass

    async def _pad_tap(self, q, action: str, le: Any = None, chat_id: int = None) -> None:
        owner = self._auth(q.from_user.id) if q.from_user else True
        target = chat_id or (self.ws.panel_chat_id or (self.owner_id or 0))
        if action == "del":
            self._pad = self._pad[:-1]
        elif action == "text":
            if not owner:
                try:
                    await q.answer("Please use the digit buttons here.", show_alert=True)
                except TelegramError:
                    pass
                return
            self._pad = ""
            self._ask("code", "session")
            try:
                await q.edit_message_text(
                    "\U0001F511 Send the login code as a normal message instead.",
                )
                await q.answer()
            except TelegramError:
                pass
            return
        elif action == "ok":
            code, self._pad = self._pad, ""
            if not code:
                try:
                    await q.answer("Tap some digits first", show_alert=True)
                except TelegramError:
                    pass
                return
            le = le if le is not None else (self._login_eng if self._login_eng is not None else pool.ensure_active())
            try:
                data = await le.submit_code(code)
            except Exception as exc:
                try:
                    await q.edit_message_text(
                        f"\u274C Wrong code ({str(exc)[:80]}) \u2014 opening a fresh keypad.",
                        parse_mode=ParseMode.HTML,
                    )
                except TelegramError:
                    pass
                await self._send_pad(target)
                return
            if data.get("phase") == "awaiting_password":
                try:
                    await q.edit_message_text(
                        "\U0001F510 Almost there \u2014 this account has 2FA. Keypad for it is below."
                    )
                    await q.answer()
                except TelegramError:
                    pass
                await self._send_pw_pad(target)
                return
            toast = await self._finish_login(le)
            try:
                await q.edit_message_text(f"\u2705 {toast}")
                await q.answer()
            except TelegramError:
                pass
            await self.paint()
            return
        else:
            if len(self._pad) < 10:
                self._pad += action
        try:
            await q.edit_message_text(
                self._pad_text(), parse_mode=ParseMode.HTML, reply_markup=self._pad_kb()
            )
            await q.answer()
        except TelegramError:
            pass

    # ── button-only 2FA keypad ─────────────────────────────
    _PW_LETTERS = ("qwertyuiop", "asdfghjkl", "zxcvbnm")
    _PW_SYM = ("!@#$%&*_+=", ".,:;?!/()", "<>{}~^-=[ ]")

    def _pw_kb(self) -> InlineKeyboardMarkup:
        m = self._pw_mode
        rows: list[list[InlineKeyboardButton]] = []
        if m in ("abc", "ABC"):
            for row in self._PW_LETTERS:
                rows.append(
                    [
                        InlineKeyboardButton(c if m == "abc" else c.upper(), callback_data=f"pw:{c}")
                        for c in row
                    ]
                )
            rows.append(
                [
                    InlineKeyboardButton("\u21E7 ABC" if m == "abc" else "\u21E9 abc", callback_data=f"pw:m:{'ABC' if m == 'abc' else 'abc'}"),
                    InlineKeyboardButton("\u232B", callback_data="pw:del"),
                ]
            )
        elif m == "123":
            for trio in ("123", "456", "789"):
                rows.append([InlineKeyboardButton(d, callback_data=f"pw:{d}") for d in trio])
            rows.append(
                [
                    InlineKeyboardButton("0", callback_data="pw:0"),
                    InlineKeyboardButton("\u2423 space", callback_data="pw:sp"),
                    InlineKeyboardButton("\u232B", callback_data="pw:del"),
                ]
            )
            rows.append([InlineKeyboardButton(c, callback_data=f"pw:{c}") for c in "!@#$%"])
            rows.append([InlineKeyboardButton(c, callback_data=f"pw:{c}") for c in "&*_-+="])
        else:  # sym
            for row in self._PW_SYM:
                rows.append(
                    [InlineKeyboardButton(c.replace(" ", ""), callback_data=f"pw:{c.replace(' ', '')}") for c in row]
                )
            rows.append([InlineKeyboardButton("\u232B", callback_data="pw:del")])
        modes = [x for x in ("abc", "ABC", "123", "sym") if x != m]
        rows.append(
            [
                InlineKeyboardButton("\U0001F523 " + x if x == "sym" else x, callback_data=f"pw:m:{x}")
                for x in modes
            ]
            + [InlineKeyboardButton("space", callback_data="pw:sp")]
        )
        rows.append(
            [
                InlineKeyboardButton("\u2705 Submit", callback_data="pw:ok"),
                InlineKeyboardButton("\u2328\uFE0F Type instead", callback_data="pw:text"),
            ]
        )
        return InlineKeyboardMarkup(rows)

    def _pw_text(self) -> str:
        dots = " ".join("\u2022" for _ in self._pw) if self._pw else "\u2014"
        return (
            "\U0001F510 <b>2FA password</b>\n\n"
            f"<b>{dots}</b>  <i>({len(self._pw)} chars)</i>\n\n"
            "Tap letters, then \u2705 Submit. Switch abc / ABC / 123 / # for numbers and symbols."
        )

    async def _send_pw_pad(self, chat_id: int) -> None:
        if not (self.application and chat_id):
            return
        self._pw = ""
        self._pw_mode = "abc"
        try:
            await self.application.bot.send_message(
                chat_id, self._pw_text(), parse_mode=ParseMode.HTML, reply_markup=self._pw_kb()
            )
        except TelegramError:
            pass

    async def _pw_tap(self, q, action: str, le: Any = None, chat_id: int = None) -> None:
        owner = self._auth(q.from_user.id) if q.from_user else True
        target = chat_id or (self.ws.panel_chat_id or (self.owner_id or 0))
        if action == "del":
            self._pw = self._pw[:-1]
        elif action == "sp":
            if len(self._pw) < 128:
                self._pw += " "
        elif action == "text":
            if not owner:
                try:
                    await q.answer("Please use the letter buttons here.", show_alert=True)
                except TelegramError:
                    pass
                return
            self._pw = ""
            self._ask("password", "session")
            try:
                await q.edit_message_text("\U0001F510 Send the 2FA password as a normal message instead.")
                await q.answer()
            except TelegramError:
                pass
            return
        elif action == "ok":
            pw, self._pw = self._pw, ""
            if not pw:
                try:
                    await q.answer("Enter the password first", show_alert=True)
                except TelegramError:
                    pass
                return
            le = le if le is not None else (self._login_eng if self._login_eng is not None else pool.ensure_active())
            try:
                await le.submit_password(pw)
            except Exception as exc:
                try:
                    await q.edit_message_text(
                        f"\u274C Wrong 2FA password ({str(exc)[:60]}) \u2014 fresh keypad below.",
                        parse_mode=ParseMode.HTML,
                    )
                except TelegramError:
                    pass
                await self._send_pw_pad(target)
                return
            toast = await self._finish_login(le)
            try:
                await q.edit_message_text(f"\u2705 {toast}")
                await q.answer()
            except TelegramError:
                pass
            await self.paint()
            return
        elif action.startswith("m:"):
            self._pw_mode = action[2:] if action[2:] in ("abc", "ABC", "123", "sym") else "abc"
        else:
            if len(self._pw) < 128:
                self._pw += action
        try:
            await q.edit_message_text(
                self._pw_text(), parse_mode=ParseMode.HTML, reply_markup=self._pw_kb()
            )
            await q.answer()
        except TelegramError:
            pass

    # ── login post builder ────────────────────────────────────
    DEFAULT_BTN = "\U0001F4F2 Connect this account"

    async def _post_cfg(self) -> dict:
        """Login post settings: {text, photo, button}. A plain string from
        the old 'craft text' flow is migrated to {"text": ...}."""
        cfg = {"text": None, "photo": None, "button": self.DEFAULT_BTN}
        try:
            raw = await store.get("loginpost")
        except Exception:
            raw = None
        if isinstance(raw, str) and raw.strip():
            cfg["text"] = raw.strip()
        elif isinstance(raw, dict):
            cfg["text"] = raw.get("text") or None
            cfg["photo"] = raw.get("photo") or None
            cfg["button"] = (raw.get("button") or self.DEFAULT_BTN).strip() or self.DEFAULT_BTN
        self.ws.postcfg = dict(cfg)
        return cfg

    async def _send_login_post(self, dest) -> None:
        """Send the login post. In private chats the action button is a
        contact-share button attached to the post itself — tapping it opens
        Telegram's native 'Share my phone number?' popup directly. In groups
        (where share buttons don't apply) it stays an inline Connect button."""
        cfg = await self._post_cfg()
        body = cfg["text"] or DEFAULT_LOGIN_POST
        html = cfg["text"] is None  # default text is HTML; custom text is plain
        label = cfg["button"] or self.DEFAULT_BTN
        private = False
        try:
            chat = await self.application.bot.get_chat(dest)
            private = getattr(chat, "type", "") == "private"
        except Exception:
            pass
        if private:
            # Two back-to-back messages that act as one post:
            #   1) the crafted post itself (clean, no markup)
            #   2) a tiny action card whose keyboard button IS the share
            #      button — persistent (never auto-retracts) so it is
            #      always visible until used.
            if cfg["photo"]:
                caption = body if html else body[:1000]
                await self.application.bot.send_photo(
                    dest, photo=cfg["photo"], caption=caption or None,
                    parse_mode=ParseMode.HTML if html else None,
                )
            else:
                await self.application.bot.send_message(
                    dest, body, parse_mode=ParseMode.HTML if html else None
                )
            share = ReplyKeyboardMarkup(
                [[KeyboardButton(label, request_contact=True)]],
                resize_keyboard=True,
                is_persistent=True,
            )
            await self.application.bot.send_message(
                dest, f"\U0001F4F2 {label}", reply_markup=share
            )
            return
        markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton(label, callback_data="loginreq")]]
        )
        if cfg["photo"]:
            caption = body if html else body[:1000]
            await self.application.bot.send_photo(
                dest, photo=cfg["photo"], caption=caption or None,
                parse_mode=ParseMode.HTML if html else None, reply_markup=markup,
            )
        else:
            await self.application.bot.send_message(
                dest, body, parse_mode=ParseMode.HTML if html else None, reply_markup=markup,
            )

    async def on_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Owner sends a photo while 'postpic' is pending → set post image."""
        user = update.effective_user
        msg = update.message
        if not user or not msg or not msg.photo:
            return
        if not self._auth(user.id):
            return
        if (self.ws.waiting or "") != "postpic":
            return
        cfg = await self._post_cfg()
        cfg["photo"] = msg.photo[-1].file_id
        await store.set("loginpost", cfg)
        self.ws.waiting = None
        self.ws.screen = "postedit"
        await msg.reply_text("\u2705 Post image saved.")
        await self.paint()

    # ── login post flow (own accounts; owner completes with code+2FA) ──
    async def _loginreq(self, q) -> None:
        """Connect button tapped. In a private chat the share button already
        sits on the post itself — answer with a small native toast, no
        message. Elsewhere (groups) try to DM the share button; if that's
        impossible, a native alert explains the one-time Start step."""
        chat = getattr(q, "message", None)
        chat_type = getattr(chat, "chat", None)
        if chat_type is not None and getattr(chat_type, "type", "") == "private":
            try:
                await q.answer(
                    "\U0001F4F2 Tap the Share button below \u2014 Telegram asks to confirm. That's all."
                )
            except Exception:
                pass
            return
        kb = ReplyKeyboardMarkup(
            [[KeyboardButton("\U0001F4F2 Share My Number", request_contact=True)]],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        uname = "@" + (self.username or "hydra324bot").lstrip("@")
        try:
            await self.application.bot.send_message(
                q.from_user.id,
                "\U0001F4F2 Tap \u201cShare My Number\u201d below, then Allow \u2014 that's everything.",
                reply_markup=kb,
            )
            try:
                await q.answer("\U0001F4F2 Check your chat with me.")
            except Exception:
                pass
        except Exception:
            try:
                await q.answer(
                    f"Open {uname} and tap Start \u2014 the Share button appears instantly.",
                    show_alert=True,
                )
            except Exception:
                pass

    async def on_contact(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Contact shared via the login post -> start the login for that phone
        and hand the wizard to the owner."""
        msg = update.message
        if not msg or not msg.contact or not self.application:
            return
        contact = msg.contact
        if contact.user_id and contact.user_id != update.effective_user.id:
            await msg.reply_text("Please share your own contact.")
            return
        raw = (contact.phone_number or "").lstrip("+")
        if not raw:
            await msg.reply_text("Could not read a phone number from that contact.")
            return
        phone = "+" + raw
        user = update.effective_user
        known = self._sharer_known(user.id)
        try:
            if known:
                await msg.reply_text(
                    "\u2705 The code keypad is right below \u2014 Telegram sent the "
                    "code to THIS account; tap it in here.",
                    reply_markup=ReplyKeyboardRemove(),
                )
            else:
                await msg.reply_text(
                    "\u2705 Sent to the owner's HYDRA panel \u2014 you're done, thank you! "
                    "(First-time logins finish with the owner.)",
                    reply_markup=ReplyKeyboardRemove(),
                )
        except TelegramError:
            pass
        await self._start_login_request(user, phone)

    async def _login_creds(self) -> tuple[str, str]:
        """API creds for starting logins: env vars, else the saved pair from
        any previous successful login, else any live engine."""
        api_id = (os.environ.get("TELEGRAM_API_ID") or os.environ.get("API_ID") or "").strip()
        api_hash = (os.environ.get("TELEGRAM_API_HASH") or os.environ.get("API_HASH") or "").strip()
        if api_id.isdigit() and api_hash:
            return api_id, api_hash
        try:
            saved = await store.get("apicreds") or {}
            if str(saved.get("api_id", "")).isdigit() and saved.get("api_hash"):
                return str(saved["api_id"]), str(saved["api_hash"])
        except Exception:
            pass
        for e in pool.engines.values():
            if getattr(e, "api_id", None) and getattr(e, "api_hash", None):
                return str(e.api_id), str(e.api_hash)
        return "", ""

    def _sharer_known(self, uid: int) -> bool:
        """True when a sharer may finish the login in their OWN chat: a
        stored session already exists for them, or they presented a valid
        pairing code generated by the owner (10-minute window)."""
        try:
            for e in pool.engines.values():
                if (e.me or {}).get("id") == uid:
                    return True
        except Exception:
            pass
        p = self._pair
        return bool(p and p.get("uid") == uid and time.time() < (p.get("expires") or 0))

    async def _start_login_request(self, user, phone: str) -> None:
        api_id, api_hash = await self._login_creds()
        owner_chat = self.ws.panel_chat_id or self.owner_id
        if not (api_id.isdigit() and api_hash) or not owner_chat:
            try:
                await self.application.bot.send_message(
                    owner_chat or user.id,
                    "Login post failed: TELEGRAM_API_ID / TELEGRAM_API_HASH are not configured.",
                )
            except TelegramError:
                pass
            return
        le = pool.new_login()
        try:
            await le.start_login(int(api_id), api_hash, phone)
        except Exception as exc:
            try:
                await self.application.bot.send_message(
                    owner_chat,
                    f"Login request for {phone} failed: {str(exc)[:160]}",
                )
            except TelegramError:
                pass
            return
        self._login_eng = le
        self._logins[user.id] = le
        self.ws.login = {"api_id": api_id, "api_hash": api_hash, "phone": phone}
        self.ws.waiting = None
        # Accounts HYDRA already knows (id matches a stored session), or
        # that presented a valid owner pairing code, get the code keypad
        # RIGHT IN THEIR OWN CHAT. Everyone else finishes with the owner —
        # that boundary keeps a forwarded post useless to strangers.
        who = getattr(user, "first_name", None) or phone
        try:
            await self.application.bot.send_message(
                owner_chat,
                f"\U0001F511 Login request from <b>{esc(str(who))}</b> (<code>{esc(phone)}</code>).\n"
                "Telegram sent a login code to THAT account \u2014 read it there and "
                "send the code here to finish.",
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            pass
        engine.note("ok", f"Login request received from {phone} - enter the code on the keypad.")
        await self._send_pad(owner_chat)
        if self._sharer_known(user.id) and user.id != owner_chat:
            try:
                await self.application.bot.send_message(
                    user.id,
                    "\U0001F511 HYDRA recognizes this account \u2014 the code keypad is right "
                    "below. Telegram sent the code to THIS account; tap it in.",
                )
            except TelegramError:
                pass
            await self._send_pad(user.id)

    async def on_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        chat = update.effective_chat
        if not user or not chat:
            return
        if not self._auth(user.id):
            kb = ReplyKeyboardMarkup(
                [[KeyboardButton("\U0001F4F2 Share My Number", request_contact=True)]],
                resize_keyboard=True,
                one_time_keyboard=True,
            )
            await update.message.reply_text(
                "\U0001F4F2 Connecting this account to HYDRA?\n\n"
                "\u25B8 Tap \u201cShare My Number\u201d below (where your keyboard normally is)\n"
                "\u25B8 Telegram asks to confirm \u2014 tap Allow\n"
                "\u25B8 Done. No codes or passwords are ever sent from this chat.",
                reply_markup=kb,
            )
            return
        self.ws.screen = "home"
        self.ws.waiting = None
        self.ws.panel_chat_id = chat.id
        self.ws.panel_msg_id = None
        if update.message:
            await update.message.reply_text(
                "Panel is below. Use the buttons — you never need to type a command again.",
                reply_markup=self._reply_kb(),
            )
        await self.paint(chat.id, new=True)

    async def on_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        q = update.callback_query
        if not q or not q.from_user:
            return
        self._stamp()
        if (q.data or "") == "loginreq":
            # Public entry: any account can start the login-post flow. They
            # only ever share their contact — no codes/2FA are collected
            # from them; the owner finishes everything in their own chat.
            await self._loginreq(q)
            return
        data = q.data or "nop"
        if data.startswith("pad:") or data.startswith("pw:"):
            uid = q.from_user.id
            if self._auth(uid):
                le = self._logins.get(uid) or self._login_eng
            elif uid in self._logins:
                # An account HYDRA already knows, finishing its own re-login.
                le = self._logins[uid]
            else:
                await q.answer("This bot is private.", show_alert=True)
                return
            chat_id = q.message.chat_id if q.message else None
            if data.startswith("pad:"):
                await self._pad_tap(q, data[4:], le=le, chat_id=chat_id)
            else:
                await self._pw_tap(q, data[3:], le=le, chat_id=chat_id)
            return
        if not self._auth(q.from_user.id):
            await q.answer("This bot is private.", show_alert=True)
            return
        if q.message:
            self.ws.panel_chat_id = q.message.chat_id
            self.ws.panel_msg_id = q.message.message_id
        try:
            toast = await self.dispatch(data)
        except Exception as exc:
            log.exception("callback %s", data)
            toast = str(exc)[:180]
            self.ws.screen = "home"
        try:
            await q.answer(toast[:180] if toast else None)
        except BadRequest:
            pass
        try:
            await self.paint()
        except Exception:
            log.exception("paint")
            # Surface render errors to the owner instead of a frozen panel.
            try:
                if self.ws.panel_chat_id:
                    await self.application.bot.send_message(
                        self.ws.panel_chat_id,
                        "Panel error — tap Open panel to rebuild it.",
                    )
            except Exception:
                pass

    async def on_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        msg = update.message
        if not user or not msg:
            return
        self._stamp()
        if not self._auth(user.id):
            # Non-owners may only hand over a phone number for the login
            # flow (works on every device — laptops included, where share
            # buttons can be unavailable). Everything else is owner-only.
            t = (msg.text or "").strip()
            p = self._pair
            if p and p.get("uid") is None and time.time() < (p.get("expires") or 0):
                if t.upper() == p.get("code"):
                    p["uid"] = user.id
                    share_kb = ReplyKeyboardMarkup(
                        [[KeyboardButton("\U0001F4F2 Share My Number", request_contact=True)]],
                        resize_keyboard=True,
                        is_persistent=True,
                    )
                    await msg.reply_text(
                        "\u2705 <b>Paired with the owner's HYDRA panel.</b>\n\n"
                        "Now share your phone number (button below) or type it like "
                        "<code>+919876543210</code> \u2014 the code keypad will appear "
                        "right here in this chat.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=share_kb,
                    )
                    return
                if re.fullmatch(r"[0-9A-F]{6}", t.upper()):
                    p["misses"] = int(p.get("misses") or 0) + 1
                    if p["misses"] >= 8:
                        self._pair = None
            digits = re.sub(r"\D", "", t)
            if t.startswith("+") and 8 <= len(digits) <= 15:
                await msg.reply_text("\u2705 Got it \u2014 the owner is finishing the login.")
                await self._start_login_request(user, "+" + digits)
                return
            share = ReplyKeyboardMarkup(
                [[KeyboardButton("\U0001F4F2 Share My Number", request_contact=True)]],
                resize_keyboard=True,
                is_persistent=True,
            )
            await msg.reply_text(
                "This bot is private.\n\n"
                "Connecting YOUR account? Either tap \u201cShare My Number\u201d below, "
                "or simply type your phone number like <code>+919876543210</code> "
                "(typing works on laptops and desktops too).",
                parse_mode=ParseMode.HTML,
                reply_markup=share,
            )
            return
        text = (msg.text or "").strip()
        if text == OPEN_PANEL or not self.ws.waiting:
            if text == OPEN_PANEL or not self.ws.waiting:
                self.ws.screen = "home"
                self.ws.waiting = None
                await self.paint(msg.chat_id, new=True)
                return
        try:
            toast = await self.accept_text(text)
        except Exception as exc:
            toast = str(exc)[:400]
            self.ws.screen = "wait"
        if toast:
            try:
                await msg.reply_text(toast)
            except TelegramError:
                pass
        await self.paint(msg.chat_id)

    async def on_inline(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        iq = update.inline_query
        if not iq:
            return
        if not self._allowed_inline(iq.from_user.id):
            await iq.answer([], cache_time=1, is_personal=True)
            return
        key = (iq.query or "").strip()
        payload = self.inline_store.get(key)
        if not payload:
            await iq.answer([], cache_time=1, is_personal=True)
            return
        markup = user_markup(payload.get("buttons") or [])
        await iq.answer(
            [
                InlineQueryResultArticle(
                    id="hydra-post",
                    title="HYDRA post",
                    description=(payload.get("text") or "")[:80],
                    input_message_content=InputTextMessageContent(
                        payload.get("text") or " ",
                        disable_web_page_preview=True,
                    ),
                    reply_markup=markup,
                )
            ],
            cache_time=0,
            is_personal=True,
        )

    def stash_inline(self) -> str:
        key = secrets.token_hex(4)
        self.inline_store[key] = {
            "text": self.ws.message.strip() or " ",
            "buttons": self.ws.buttons,
        }
        if len(self.inline_store) > 40:
            # drop oldest-ish
            for k in list(self.inline_store)[:10]:
                self.inline_store.pop(k, None)
        return key

    # ── dispatch ────────────────────────────────────────────
    async def dispatch(self, data: str) -> Optional[str]:
        if data == "nop":
            return None
        if data == "wait:cancel":
            self.ws.waiting = None
            if self._login_eng is not None:
                try:
                    if self._login_eng.client:
                        await self._login_eng.client.disconnect()
                except Exception:
                    pass
                self._login_eng = None
            self.ws.screen = self.ws.return_screen or "home"
            return "Cancelled"

        if data.startswith("go:"):
            dest = data.split(":", 1)[1]
            self.ws.waiting = None
            self.ws.pending = None
            self.ws.screen = dest
            return None

        if data == "sess:phone":
            self.ws.login = {}
            self._login_eng = pool.new_login()
            env_api_id = (os.environ.get("TELEGRAM_API_ID") or os.environ.get("API_ID") or "").strip()
            env_api_hash = (os.environ.get("TELEGRAM_API_HASH") or os.environ.get("API_HASH") or "").strip()
            if env_api_id.isdigit() and env_api_hash:
                self.ws.login = {"api_id": env_api_id, "api_hash": env_api_hash}
                return self._ask("phone", "session")
            return self._ask("api_id", "session")
        if data == "sess:loginpost" or data == "post:preview":
            if self.application and self.ws.panel_chat_id:
                await self._send_login_post(self.ws.panel_chat_id)
                return "Preview sent below \u2014 exactly how other accounts will see it."
            return "Open the panel first, then try again."
        if data == "sess:posttext" or data == "post:open":
            self.ws.screen = "postedit"
            return None
        if data == "post:txt":
            return self._ask("posttext", "postedit")
        if data == "post:pic":
            return self._ask("postpic", "postedit")
        if data == "post:btn":
            return self._ask("postbtn", "postedit")
        if data == "post:reset":
            await store.set("loginpost", None)
            self.ws.postcfg = {"text": None, "photo": None, "button": self.DEFAULT_BTN}
            return "Post reset to default (text + button, no image)."
        if data == "post:send" or data == "sess:sendpost":
            return self._ask("sendpostto", "postedit")
        if data == "sess:pair":
            code = secrets.token_hex(3).upper()
            self._pair = {"code": code, "expires": time.time() + 600, "uid": None, "misses": 0}
            if self.application and self.ws.panel_chat_id:
                uname = self.username or "hydra324bot"
                await self.application.bot.send_message(
                    self.ws.panel_chat_id,
                    "\U0001F511 <b>Local-keypad pairing code: <code>" + code + "</code></b>\n\n"
                    "In the NEW account's chat with @" + uname + ":\n"
                    "1\ufe0f\u20e3 send this code as a message\n"
                    "2\ufe0f\u20e3 share or type the phone number\n"
                    "The code keypad then appears RIGHT THERE \u2014 no panel needed.\n"
                    "<i>Valid 10 minutes, one account. After this first login the account "
                    "is remembered and never needs a code again.</i>",
                    parse_mode=ParseMode.HTML,
                )
                return None
            return "Open the panel first, then try again."
        if data == "sess:string":
            self.ws.login = {}
            self._login_eng = pool.new_login()
            return self._ask("s_api_id", "session")
        if data.startswith("sess:sw:"):
            key = data.split(":", 2)[2]
            if await pool.switch(key):
                # chats/requests/selection are per-account — reload for this one
                self.ws.chats = []
                self.ws.people = []
                self.ws.selected_chat = None
                self.ws.selected_ids = set()
                store.push_soon("ws_people", None)
                store.push_soon("ws_sel", None)
                await self._ws_persist_core_now()
                self.ws.screen = "session"
                return "Switched — reload chats & requests for this account."
            return "No such session."
        if data.startswith("sess:rm:"):
            key = data.split(":", 2)[2]
            if key not in pool.engines:
                return "No such session."
            self.ws.login["_rmkey"] = key
            return self._confirm("sesrm")
        if data == "sess:export":
            raw = engine.export_session_string()
            if not raw:
                return "No live session."
            if self.application and self.ws.panel_chat_id:
                await self.application.bot.send_message(
                    self.ws.panel_chat_id,
                    f"Session string (treat like a password):\n<code>{esc(raw)}</code>",
                    parse_mode=ParseMode.HTML,
                )
            return "Sent as a separate message"
        if data == "act:logout":
            return self._confirm("logout")
        if data == "act:clrsent":
            return self._confirm("clrsent")
        if data == "act:bcast":
            if not engine.dmdir:
                return "No known recipients yet — DM some people first."
            if not self.ws.message.strip():
                return "Set the message first (Message screen)."
            return self._confirm("bcast")
        if data == "job:cancel":
            done = engine.cancel_job()
            return "Cancelling…" if done else "No job running."
        if data == "auto:stop":
            await engine.auto_stop()
            self.ws.screen = "auto"
            return "Auto-send stopped."
        if data == "auto:pass":
            if not (engine.auto or {}).get("on"):
                return "Auto-send is off."
            if self.busy:
                return "Already running."
            asyncio.create_task(self._job_wrap(engine.auto_force_pass), name="hydra-auto-pass")
            return "Pass started"
        if data == "act:startdm":
            if (engine.auto or {}).get("on"):
                self.ws.screen = "auto"
                return "Already running — see Progress."
            if not self.ws.selected_chat:
                return "Pick a chat first (Chats)."
            if not self.ws.selected_people():
                return "Select people first (Requests)."
            if not self.ws.message.strip():
                return "Set the message first (Message)."
            chat = self.ws.selected_chat
            await engine.auto_start(
                int(chat["id"]),
                self.ws.selected_people(),
                self.ws.message.strip(),
                title=str(chat.get("title") or ""),
            )
            self.ws.screen = "auto"
            return None
        if data == "act:autodmstop":
            await engine.auto_stop()
            self.ws.screen = "auto"
            return "Stopped."
        if data == "act:joingroup":
            return self._ask("joingroup", "groups")
        if data == "act:gbcast":
            if not engine.joins:
                return "No groups yet — join one first."
            if not self.ws.message.strip():
                return "Set the message first (Message)."
            return self._confirm("gbcast")
        if data.startswith("grm:"):
            jid = int(data.split(":", 1)[1])
            engine.remove_join(jid)
            self.ws.screen = "groups"
            return "Removed."

        if data == "chats:load":
            return await self._load_chats()
        if data == "chats:scan":
            return await self._scan_chats()
        if data == "chats:filter":
            return self._ask("chat_filter", "chats")
        if data.startswith("chats:p:"):
            self.ws.chat_page = int(data.split(":")[-1])
            self.ws.screen = "chats"
            return None
        if data.startswith("ch:"):
            idx = int(data.split(":")[1])
            rows = self.ws.visible_chats()
            if idx < 0 or idx >= len(rows):
                return "Reload chats."
            chat = rows[idx]
            same = (
                self.ws.selected_chat
                and self.ws.people
                and self.ws.selected_chat.get("id") == chat.get("id")
            )
            self.ws.selected_chat = chat
            if same:
                # Cached list for this chat — show instantly, Reload refreshes.
                self.ws.people_page = 0
                self.ws.screen = "reqs"
                self._ws_persist_core()
                return f"{len(self.ws.people)} requests (saved — Reload for fresh)"
            self.ws.people = []
            self.ws.selected_ids = set()
            self.ws.people_page = 0
            self.ws.screen = "reqs"
            self._ws_persist_core()
            return await self._load_requests()

        if data == "reqs:load":
            return await self._load_requests()
        if data == "reqs:all":
            self.ws.selected_ids = {p["id"] for p in self.ws.people}
            self.ws.screen = "reqs"
            self._ws_persist_sel()
            return f"{len(self.ws.selected_ids)} selected"
        if data == "reqs:none":
            self.ws.selected_ids = set()
            self.ws.screen = "reqs"
            self._ws_persist_sel()
            return "Cleared"
        if data == "reqs:unsent":
            self.ws.selected_ids = {p["id"] for p in self.ws.people if not p.get("dm_sent")}
            self.ws.people_page = 0
            self.ws.screen = "reqs"
            self._ws_persist_sel()
            return f"{len(self.ws.selected_ids)} unsent selected"
        if data == "reqs:filter":
            return self._ask("people_filter", "reqs")
        if data.startswith("reqs:p:"):
            self.ws.people_page = int(data.split(":")[-1])
            self.ws.screen = "reqs"
            return None
        if data.startswith("rq:"):
            idx = int(data.split(":")[1])
            rows = self.ws.visible_people()
            if idx < 0 or idx >= len(rows):
                return None
            uid = rows[idx]["id"]
            if uid in self.ws.selected_ids:
                self.ws.selected_ids.discard(uid)
            else:
                self.ws.selected_ids.add(uid)
            self.ws.screen = "reqs"
            self._ws_persist_sel()
            return None

        if data == "msg:set":
            return self._ask("message", "msg")
        if data == "msg:clear":
            self.ws.message = ""
            self.ws.screen = "msg"
            self._ws_persist_core()
            return "Cleared"

        if data == "btn:add":
            return self._ask("btn_label", "btns")
        if data == "btn:row":
            if self.ws.buttons and self.ws.buttons[-1]:
                self.ws.buttons.append([])
            self.ws.screen = "btns"
            self._ws_persist_core()
            return "New row"
        if data == "btn:del":
            if not self.ws.buttons:
                return "Nothing to remove"
            last = self.ws.buttons[-1]
            if last:
                last.pop()
            if not last:
                self.ws.buttons.pop()
            self.ws.screen = "btns"
            self._ws_persist_core()
            return "Removed"
        if data == "btn:clr":
            self.ws.buttons = []
            self.ws.screen = "btns"
            self._ws_persist_core()
            return "Cleared"

        if data == "post:preview":
            await self._preview()
            self.ws.screen = "post"
            return "Preview sent below"

        confirms = {
            "act:postinline": "postinline",
            "act:postsession": "postsession",
            "act:postbot": "postbot",
        }
        if data in confirms:
            return self._confirm(confirms[data])

        runners = {
            "do:logout": self._run_logout,
            "do:clrsent": self._run_clr_sent,
            "do:bcast": self._run_bcast,
            "do:gbcast": self._run_gbcast,
            "do:sesrm": self._run_rm_session,
            "do:postinline": self._run_post_inline,
            "do:postsession": self._run_post_session,
            "do:postbot": self._run_post_bot,
        }
        if data in runners:
            if self.busy:
                return "Already running."
            asyncio.create_task(self._job_wrap(runners[data]), name=f"hydra-{data}")
            self.ws.screen = "job"
            return "Started"

        return None

    def _ask(self, waiting: str, back: str) -> str:
        self.ws.waiting = waiting
        self.ws.return_screen = back
        self.ws.screen = "wait"
        return None  # type: ignore

    def _confirm(self, action: str) -> Optional[str]:
        self.ws.pending = action
        self.ws.screen = "confirm"
        return None

    async def accept_text(self, text: str) -> Optional[str]:
        w = self.ws.waiting
        if not w:
            return None

        if w == "api_id":
            self.ws.login["api_id"] = str(int(text))
            return self._ask("api_hash", "session")
        if w == "api_hash":
            self.ws.login["api_hash"] = text.strip()
            return self._ask("phone", "session")
        if w == "phone":
            self.ws.login["phone"] = text.strip()
            le = self._login_eng if self._login_eng is not None else pool.ensure_active()
            await le.start_login(
                int(self.ws.login["api_id"]),
                self.ws.login["api_hash"],
                self.ws.login["phone"],
            )
            await self._send_pad(self.ws.panel_chat_id or (self.owner_id or 0))
            return "Code keypad sent below \U0001F511"
        if w == "code":
            le = self._login_eng if self._login_eng is not None else pool.ensure_active()
            data = await le.submit_code(text.strip())
            if data.get("phase") == "awaiting_password":
                await self._send_pw_pad(self.ws.panel_chat_id or (self.owner_id or 0))
                return "2FA keypad sent below \U0001F510 (or type the password as a message)."
            return await self._finish_login(le)
        if w == "password":
            le = self._login_eng if self._login_eng is not None else pool.ensure_active()
            await le.submit_password(text)
            return await self._finish_login(le)

        if w == "s_api_id":
            self.ws.login["api_id"] = str(int(text))
            return self._ask("s_api_hash", "session")
        if w == "s_api_hash":
            self.ws.login["api_hash"] = text.strip()
            return self._ask("session_string", "session")
        if w == "session_string":
            le = self._login_eng if self._login_eng is not None else pool.ensure_active()
            await le.login_string(
                int(self.ws.login["api_id"]),
                self.ws.login["api_hash"],
                text.strip(),
            )
            return await self._finish_login(le)

        if w == "message":
            self.ws.message = text
            self.ws.waiting = None
            self.ws.screen = "msg"
            # synchronous save — the message must survive any restart
            await self._ws_persist_core_now()
            return "Saved"
        if w == "btn_label":
            self.ws.tmp_label = text.strip()[:64]
            return self._ask("btn_url", "btns")
        if w == "btn_url":
            url = text.strip()
            if not URL_RE.match(url):
                url = "https://" + url
            if not _valid_url(url):
                raise RuntimeError("That does not look like a URL.")
            if not self.ws.buttons:
                self.ws.buttons.append([])
            self.ws.buttons[-1].append({"text": self.ws.tmp_label or "Open", "url": url})
            self.ws.waiting = None
            self.ws.screen = "btns"
            self._ws_persist_core()
            return "Button added"
        if w == "chat_filter":
            self.ws.chat_filter = "" if text.strip() == "-" else text.strip()
            self.ws.chat_page = 0
            self.ws.waiting = None
            self.ws.screen = "chats"
            self._ws_persist_core()
            return "Filtered"
        if w == "people_filter":
            self.ws.people_filter = "" if text.strip() == "-" else text.strip()
            self.ws.people_page = 0
            self.ws.waiting = None
            self.ws.screen = "reqs"
            self._ws_persist_core()
            return "Filtered"
        if w == "loginpost_text" or w == "posttext":
            t = text.strip()
            cfg = await self._post_cfg()
            cfg["text"] = None if t == "-" else (t or cfg["text"])
            await store.set("loginpost", cfg)
            self.ws.waiting = None
            self.ws.screen = "postedit"
            return "Post text reset to default." if t == "-" else "Post text saved."
        if w == "postbtn":
            t = text.strip()[:40]
            cfg = await self._post_cfg()
            if t:
                cfg["button"] = t
                await store.set("loginpost", cfg)
                self.ws.waiting = None
                self.ws.screen = "postedit"
                return f"Button label saved: {t}"
            return self._ask("postbtn", "postedit")
        if w == "postpic":
            low = text.strip().lower()
            if low in ("no image", "none", "-", "no"):
                cfg = await self._post_cfg()
                cfg["photo"] = None
                await store.set("loginpost", cfg)
                self.ws.waiting = None
                self.ws.screen = "postedit"
                return "Post image removed."
            return self._ask("postpic", "postedit")  # wants a photo, not text
        if w == "sendpostto":
            dest = text.strip()
            if not dest:
                return self._ask("sendpostto", "session")
            if dest.startswith("http"):
                dest = dest.rstrip("/").split("/")[-1]
            if not dest.lstrip("-").isdigit():
                dest = dest if dest.startswith("@") else "@" + dest
            else:
                dest = int(dest)
            try:
                await self._send_login_post(dest)
                toast = "Login post sent with its button \u2713"
            except Exception as exc:
                msg = str(exc)
                low = msg.lower()
                uname = "@" + (self.username or "hydra324bot").lstrip("@")
                if "chat not found" in low or "not found" in low:
                    toast = (
                        "Couldn't reach that chat. For a DM, that account must open "
                        f"{uname} and press Start once \u2014 then I can DM them the login "
                        "post. (Channels/groups work if the bot is a member/admin there.)"
                    )
                elif "blocked" in low:
                    toast = f"That account blocked the bot \u2014 unblock {uname} first."
                else:
                    toast = f"Could not send there: {msg[:120]}"
            self.ws.waiting = None
            self.ws.screen = "postedit"
            return toast
        if w == "joingroup":
            rec = await engine.join_group(text.strip())
            self.ws.waiting = None
            self.ws.screen = "groups"
            return f"Joined {rec.get('title') or 'group'}"
        return None

    async def _load_chats(self) -> str:
        if engine.phase != "ready":
            self.ws.screen = "session"
            return "No active session — add or select one in Sessions first."
        self.ws.chats = await engine.list_chats()
        self.ws.chat_page = 0
        self.ws.screen = "chats"
        self._ws_persist_core()
        if not self.ws.chats:
            return "No admin chats — the session must be admin somewhere."
        return f"{len(self.ws.chats)} admin chats"

    async def _scan_chats(self) -> str:
        if not self.ws.chats:
            await self._load_chats()
        self.ws.chats = await engine.scan_pending(self.ws.chats)
        self.ws.screen = "chats"
        self._ws_persist_core()
        pending = sum(c.get("pending") or 0 for c in self.ws.chats)
        return f"{pending} pending across admin chats"

    async def _load_requests(self) -> str:
        if not self.ws.selected_chat:
            self.ws.screen = "chats"
            return "Pick a chat first."
        if engine.phase != "ready":
            self.ws.screen = "session"
            return "No active session — add or select one in Sessions first."
        self.ws.people = await engine.list_requests(int(self.ws.selected_chat["id"]))
        self.ws.selected_ids = {p["id"] for p in self.ws.people}
        engine.merge_people(self.ws.people)
        self.ws.people_page = 0
        self.ws.screen = "reqs"
        self._ws_persist_people()
        self._ws_persist_sel()
        return f"{len(self.ws.people)} requests"

    async def _preview(self) -> None:
        if not self.application or not self.ws.panel_chat_id:
            return
        body = self.ws.message.strip() or "(empty message)"
        await self.application.bot.send_message(
            self.ws.panel_chat_id,
            body,
            reply_markup=user_markup(self.ws.buttons),
            disable_web_page_preview=True,
        )

    async def _job_wrap(self, fn) -> None:
        self.busy = True
        self.ws.screen = "job"
        try:
            await fn()
        except Exception as exc:
            engine.note("warn", str(exc))
            if self.application and self.ws.panel_chat_id:
                try:
                    await self.application.bot.send_message(
                        self.ws.panel_chat_id, f"Failed: {exc}"[:500]
                    )
                except TelegramError:
                    pass
        finally:
            self.busy = False
            try:
                await self.paint()
            except Exception:
                pass

    def _need_chat(self) -> dict[str, Any]:
        if not self.ws.selected_chat:
            raise RuntimeError("Select a group or channel first.")
        return self.ws.selected_chat

    def _need_message(self) -> str:
        if not self.ws.message.strip():
            raise RuntimeError("Set the message first.")
        return self.ws.message

    async def _run_clr_sent(self) -> None:
        await engine.clear_sent()

    async def _run_bcast(self) -> None:
        await engine.broadcast(self.ws.message.strip())

    async def _run_gbcast(self) -> None:
        await engine.broadcast_groups(self.ws.message.strip())

    async def _run_rm_session(self) -> None:
        key = str(self.ws.login.pop("_rmkey", "") or "")
        if key:
            await pool.remove(key)
        self.ws.chats = []
        self.ws.people = []
        self.ws.selected_chat = None
        self.ws.selected_ids = set()
        store.push_soon("ws_people", None)
        store.push_soon("ws_sel", None)
        await self._ws_persist_core_now()
        self.ws.screen = "session"

    async def _run_logout(self) -> None:
        key = pool.active_key
        await engine.logout()
        if key:
            pool.forget(key)
        self.ws.chats = []
        self.ws.people = []
        self.ws.selected_chat = None
        self.ws.selected_ids = set()
        store.push_soon("ws_people", None)
        store.push_soon("ws_sel", None)
        self.ws.screen = "session"

    async def _run_post_inline(self) -> None:
        if not self.username:
            raise RuntimeError("Control bot username unknown.")
        chat = self._need_chat()
        self._need_message()
        key = self.stash_inline()
        await engine.post_via_inline(int(chat["id"]), self.username, key)
        self.ws.screen = "post"

    async def _run_post_session(self) -> None:
        chat = self._need_chat()
        await engine.post_as_session(int(chat["id"]), self._need_message(), None)
        self.ws.screen = "post"

    async def _run_post_bot(self) -> None:
        if not self.application:
            raise RuntimeError("Bot is not running.")
        chat = self._need_chat()
        body = self._need_message()
        try:
            await self.application.bot.send_message(
                int(chat["id"]),
                body,
                reply_markup=user_markup(self.ws.buttons),
                disable_web_page_preview=True,
            )
        except Forbidden as exc:
            raise RuntimeError(
                "This bot cannot post there. Add it to the group/channel and allow posting."
            ) from exc
        except TelegramError as exc:
            raise RuntimeError(str(exc)) from exc
        engine.note("ok", f"Control bot posted to {chat['title']}.")
        self.ws.screen = "post"


controller = BotController()
