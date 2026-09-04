from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

from telegram import (
    BotCommand,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    KeyboardButton,
    MenuButtonCommands,
    ReplyKeyboardMarkup,
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
from hydra.engine import DATA, engine
from hydra.panel import esc, render

log = logging.getLogger("hydra.bot")
BOT_PATH = DATA / "bot.json"
URL_RE = re.compile(r"^https?://", re.I)

OPEN_PANEL = "Open panel"


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
        return {
            "running": self.running,
            "username": self.username,
            "owner_id": self.owner_id,
            "has_token": bool(self.token or self._load().get("token")),
        }

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
                drop_pending_updates=True,
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

    def _register(self, app: Application) -> None:
        app.add_handler(CommandHandler("start", self.on_start))
        app.add_handler(CallbackQueryHandler(self.on_callback))
        app.add_handler(InlineQueryHandler(self.on_inline))
        app.add_handler(
            MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, self.on_text)
        )

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
    async def on_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        chat = update.effective_chat
        if not user or not chat:
            return
        if not self._auth(user.id):
            await update.message.reply_text("This bot is private.")
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
        if not self._auth(q.from_user.id):
            await q.answer("This bot is private.", show_alert=True)
            return
        if q.message:
            self.ws.panel_chat_id = q.message.chat_id
            self.ws.panel_msg_id = q.message.message_id
        data = q.data or "nop"
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

    async def on_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        msg = update.message
        if not user or not msg:
            return
        if not self._auth(user.id):
            await msg.reply_text("This bot is private.")
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
            env_api_id = (os.environ.get("TELEGRAM_API_ID") or os.environ.get("API_ID") or "").strip()
            env_api_hash = (os.environ.get("TELEGRAM_API_HASH") or os.environ.get("API_HASH") or "").strip()
            if env_api_id.isdigit() and env_api_hash:
                self.ws.login = {"api_id": env_api_id, "api_hash": env_api_hash}
                return self._ask("phone", "session")
            return self._ask("api_id", "session")
        if data == "sess:string":
            self.ws.login = {}
            return self._ask("s_api_id", "session")
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
        if data == "act:autoint":
            return self._ask("autoint", "act")
        if data == "act:bcast":
            if not engine.dmdir:
                return "No known recipients yet — DM some people first."
            if not self.ws.message.strip():
                return "Set the message first (Message screen)."
            return self._confirm("bcast")
        if data == "job:cancel":
            done = engine.cancel_job()
            return "Cancelling…" if done else "No job running."
        if data == "act:auto":
            if engine.auto.get("on"):
                await engine.auto_stop()
                return "Auto-send stopped."
            if not self.ws.selected_chat:
                return "Pick a chat first."
            if not self.ws.selected_people():
                return "Select requesters first (Requests screen)."
            if not self.ws.message.strip():
                return "Set the message first (Message screen)."
            return self._confirm("auto")

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
            "act:arm": "arm",
            "act:fire": "fire",
            "act:send": "send",
            "act:inlinedm": "inlinedm",
            "act:disarm": "disarm",
            "act:postinline": "postinline",
            "act:postsession": "postsession",
            "act:postbot": "postbot",
        }
        if data in confirms:
            return self._confirm(confirms[data])

        runners = {
            "do:arm": self._run_arm,
            "do:fire": self._run_fire,
            "do:send": self._run_send,
            "do:inlinedm": self._run_inline_dm,
            "do:disarm": self._run_disarm,
            "do:logout": self._run_logout,
            "do:clrsent": self._run_clr_sent,
            "do:auto": self._run_auto_on,
            "do:bcast": self._run_bcast,
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
            await engine.start_login(
                int(self.ws.login["api_id"]),
                self.ws.login["api_hash"],
                self.ws.login["phone"],
            )
            return self._ask("code", "session")
        if w == "code":
            data = await engine.submit_code(text.strip())
            if data.get("phase") == "awaiting_password":
                return self._ask("password", "session")
            self.ws.waiting = None
            self.ws.screen = "home"
            return "Session live."
        if w == "password":
            await engine.submit_password(text)
            self.ws.waiting = None
            self.ws.screen = "home"
            return "Session live."

        if w == "s_api_id":
            self.ws.login["api_id"] = str(int(text))
            return self._ask("s_api_hash", "session")
        if w == "s_api_hash":
            self.ws.login["api_hash"] = text.strip()
            return self._ask("session_string", "session")
        if w == "session_string":
            await engine.login_string(
                int(self.ws.login["api_id"]),
                self.ws.login["api_hash"],
                text.strip(),
            )
            self.ws.waiting = None
            self.ws.screen = "home"
            return "Session live."

        if w == "message":
            self.ws.message = text
            self.ws.waiting = None
            self.ws.screen = "msg"
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
        if w == "autoint":
            try:
                n = max(10, min(1440, int(text.strip())))
            except ValueError:
                return self._ask("autoint", "act")
            engine.auto_set_interval(n)
            self.ws.waiting = None
            self.ws.screen = "act"
            return f"Auto-send interval: every {n} min"
        return None

    async def _load_chats(self) -> str:
        self.ws.chats = await engine.list_chats()
        self.ws.chat_page = 0
        self.ws.screen = "chats"
        self._ws_persist_core()
        return f"{len(self.ws.chats)} chats"

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

    async def _run_arm(self) -> None:
        chat = self._need_chat()
        await engine.arm_drafts(int(chat["id"]), self.ws.selected_people(), self._need_message())

    async def _run_fire(self) -> None:
        await engine.fire_drafts()

    async def _run_send(self) -> None:
        chat = self._need_chat()
        await engine.send_now(int(chat["id"]), self.ws.selected_people(), self._need_message())

    async def _run_inline_dm(self) -> None:
        if not self.username:
            raise RuntimeError("Control bot username unknown.")
        chat = self._need_chat()
        key = self.stash_inline()
        await engine.dm_via_inline(
            self.ws.selected_people(),
            self.username,
            key,
            int(chat["id"]),
            self._need_message(),
        )

    async def _run_disarm(self) -> None:
        await engine.disarm()

    async def _run_clr_sent(self) -> None:
        await engine.clear_sent()

    async def _run_auto_on(self) -> None:
        chat = self.ws.selected_chat or {}
        await engine.auto_start(
            int(chat["id"]),
            self.ws.selected_people(),
            self.ws.message.strip(),
        )

    async def _run_bcast(self) -> None:
        await engine.broadcast(self.ws.message.strip())

    async def _run_logout(self) -> None:
        await engine.logout()
        self.ws.chats = []
        self.ws.people = []
        self.ws.selected_chat = None
        self.ws.selected_ids = set()
        store.push_soon("ws", None)
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
