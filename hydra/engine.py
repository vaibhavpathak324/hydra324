from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from telethon import TelegramClient
from telethon.errors import (
    ChatAdminRequiredError,
    FloodWaitError,
    InputUserDeactivatedError,
    MultiError,
    PeerFloodError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
    UserIsBlockedError,
    UserPrivacyRestrictedError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetAdminedPublicChannelsRequest

from hydra import store
from telethon.tl.functions.messages import (
    GetAllDraftsRequest,
    GetChatInviteImportersRequest,
    SaveDraftRequest,
    SendMessageRequest,
)
from telethon.tl.types import (
    Channel,
    Chat,
    DraftMessage,
    InputPeerUser,
    InputUser,
    InputUserEmpty,
    PeerUser,
    UpdateDraftMessage,
    User,
)

log = logging.getLogger("hydra")

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CREDS_PATH = DATA / "creds.json"
SESSION_PATH = DATA / "session.string"

# MTProto containers stay reliable around this size even with medium-length copy.
BURST = 48
# Pacing between MTProto containers. Drafts are cheap; keep this snappy but
# not so fast that Telegram flood-waits every container.
BURST_PACE = 0.35
# Draft writing is much cheaper for Telegram than sending — run it wider.
ARM_BURST = 64
ARM_PACE = 0.15
# Flood policy: NO waiting. When Telegram demands a flood wait the item is
# instantly marked flood_wait (retryable later) and the job moves on. Rapid
# re-passes (auto-send every few seconds) pick items up as Telegram allows.
FLOOD_RETRIES = 0
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _display_name(user: User | None, fallback_id: int = 0) -> str:
    if user is None:
        return f"user:{fallback_id}"
    parts = [user.first_name or "", user.last_name or ""]
    name = " ".join(p for p in parts if p).strip()
    return name or (f"@{user.username}" if user.username else f"user:{user.id}")


def _is_disconnect(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return isinstance(exc, ConnectionError) or "disconnect" in msg or "not connected" in msg


def _err_name(exc: BaseException) -> str:
    if isinstance(exc, UserPrivacyRestrictedError):
        return "privacy"
    if isinstance(exc, PeerFloodError):
        return "peer_flood"
    if isinstance(exc, UserIsBlockedError):
        return "blocked"
    if isinstance(exc, InputUserDeactivatedError):
        return "deactivated"
    if isinstance(exc, FloodWaitError):
        return f"flood_wait:{getattr(exc, 'seconds', '?')}"
    text = str(exc) or exc.__class__.__name__
    return text[:160]


@dataclass
class ArmedTarget:
    user_id: int
    access_hash: int
    name: str
    username: Optional[str]
    message: str
    from_chat_id: int


@dataclass
class Job:
    id: str
    kind: str
    total: int = 0
    done: int = 0
    ok: int = 0
    fail: int = 0
    skipped: int = 0
    status: str = "running"
    detail: str = ""
    cancel_requested: bool = False
    touched: float = field(default_factory=time.time)
    errors: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "total": self.total,
            "done": self.done,
            "ok": self.ok,
            "fail": self.fail,
            "skipped": self.skipped,
            "status": self.status,
            "detail": self.detail,
            "errors": self.errors[-40:],
        }


class HydraEngine:
    def __init__(self, skey: str = "main") -> None:
        # Per-session key: state in the Supabase store is namespaced by this
        # ("main" keeps the legacy unprefixed keys so existing data migrates).
        self.skey = skey
        if skey == "main":
            self.creds_path = DATA / "creds.json"
            self.session_path = DATA / "session.string"
        else:
            safe = "".join(ch for ch in skey if ch.isalnum() or ch in "-_")
            self.creds_path = DATA / f"creds-{safe}.json"
            self.session_path = DATA / f"session-{safe}.string"
        self.client: Optional[TelegramClient] = None
        self.api_id: Optional[int] = None
        self.api_hash: Optional[str] = None
        self.phone: Optional[str] = None
        self.phone_code_hash: Optional[str] = None
        self.me: Optional[dict[str, Any]] = None
        # user_ids this account has already DMed — never double-send (persisted).
        self.sent_ids: set[int] = set()
        # Everyone this account has ever DMed, with access hashes — the
        # broadcast list (persisted, account-bound).
        self.dmdir: dict[int, dict[str, Any]] = {}
        # Auto-send job state (persisted): keeps DMing the unsent selection.
        self.auto: dict[str, Any] = {}
        self._auto_task: Optional[asyncio.Task] = None
        self.phase: str = "logged_out"
        self.armed: list[ArmedTarget] = []
        self.job: Optional[Job] = None
        self.logs: deque[dict[str, Any]] = deque(maxlen=300)
        self._listeners: set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()
        self._job_lock = asyncio.Lock()
        self._conn_lock = asyncio.Lock()
        self._keep_task: Optional[asyncio.Task] = None
        # Chat list cache: Reload cooldown + stale fallback on Telegram floods.
        self._chats_cache: Optional[list[dict[str, Any]]] = None
        self._chats_ts: float = 0.0
        self._chats_lock = asyncio.Lock()

    def _k(self, name: str) -> str:
        return name if self.skey == "main" else f"{self.skey}:{name}"

    def _make_client(self, session: StringSession) -> TelegramClient:
        client = TelegramClient(
            session,
            self.api_id,
            self.api_hash,
            device_model="HYDRA 324",
            system_version="Linux",
            app_version="0.1.0",
            connection_retries=5,
            retry_delay=1,
            auto_reconnect=True,
            timeout=15,
        )
        # Raise FloodWaitError instead of sleeping invisibly so the UI can show it.
        client.flood_sleep_threshold = 0
        return client

    # ── events ──────────────────────────────────────────────
    def listen(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._listeners.add(q)
        return q

    def unlisten(self, q: asyncio.Queue) -> None:
        self._listeners.discard(q)

    def emit(self, event: str, **payload: Any) -> None:
        msg = {"event": event, **payload}
        dead: list[asyncio.Queue] = []
        for q in self._listeners:
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            self._listeners.discard(q)

    def note(self, level: str, text: str, **extra: Any) -> None:
        entry = {"ts": _now(), "level": level, "text": text, **extra}
        self.logs.append(entry)
        self.emit("log", log=entry)
        log.info("%s %s", level.upper(), text)

    def snapshot(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "me": self.me,
            "armed": len(self.armed),
            "sent": len(self.sent_ids),
            "dir": len(self.dmdir),
            "draft_msg": (self.armed[0].message[:300] if self.armed else ""),
            "auto": self.auto_status(),
            "job": self.job.as_dict() if self.job else None,
        }

    # ── persistence ─────────────────────────────────────────
    def _write_creds(self) -> None:
        DATA.mkdir(parents=True, exist_ok=True)
        self.creds_path.write_text(
            json.dumps(
                {
                    "api_id": self.api_id,
                    "api_hash": self.api_hash,
                    "phone": self.phone,
                }
            )
        )

    def _write_session_string(self) -> None:
        if not self.client:
            return
        DATA.mkdir(parents=True, exist_ok=True)
        raw = self.client.session.save()
        self.session_path.write_text(raw)

    def export_session_string(self) -> Optional[str]:
        if not self.client:
            return None
        return self.client.session.save()

    async def try_resume(self) -> bool:
        """Restore the saved session. Returns True when the session is live.

        Retries twice on transient failures (network/DC hiccups at boot) —
        giving up on the first blip left the bot 'logged out' until a
        redeploy, which is exactly what we want to avoid.
        """
        for attempt in range(1, 4):
            # Ephemeral filesystem (e.g. Render free tier): restore from the
            # Supabase state store before giving up.
            if not self.creds_path.exists() or not self.session_path.exists():
                if not await store.ensure_ready():
                    return False  # store unreachable — self-heal will retry
                await store.pull_to_file(self._k("creds"), self.creds_path)
                await store.pull_to_file(self._k("session"), self.session_path)
            if not self.creds_path.exists() or not self.session_path.exists():
                return False  # nothing stored for this slot yet
            try:
                creds = json.loads(self.creds_path.read_text())
                session = self.session_path.read_text().strip()
                if not session or not creds.get("api_id"):
                    return False
                self.api_id = int(creds["api_id"])
                self.api_hash = creds["api_hash"]
                self.phone = creds.get("phone")
                self.client = self._make_client(StringSession(session))
                await self.client.connect()
                if await self.client.is_user_authorized():
                    await self._mark_online()
                    self.note("ok", "Resumed existing session.")
                    await self._restore_state()
                    self.start_background()
                    return True
                # Genuinely unauthorized (revoked/logged out elsewhere)
                await self.client.disconnect()
                self.client = None
                self.phase = "logged_out"
                self.note("warn", "Session is no longer authorized — log in again.")
                return False
            except Exception as exc:
                self.note(
                    "warn",
                    f"Could not resume session (attempt {attempt}/3): {_err_name(exc)}"
                    + (" — retrying…" if attempt < 3 else " — will keep retrying in the background."),
                )
                if self.client:
                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass
                self.client = None
                self.phase = "logged_out"
                await asyncio.sleep(3 * attempt)
        return False

    async def _restore_state(self) -> None:
        """Rebuild armed drafts + DM history from the state store (survives restarts)."""
        try:
            armed_uid = await store.get(self._k("armed_uid"))
            if armed_uid is None or armed_uid == (self.me or {}).get("id"):
                raw = await store.get(self._k("armed")) or []
                self.armed = [ArmedTarget(**a) for a in raw]
            else:
                self.armed = []  # armed list belonged to a different account
        except Exception:
            self.armed = []
        if self.armed:
            self.note("ok", f"Restored {len(self.armed)} armed drafts — Send all drafts to resume them.")
        try:
            sent = await store.get(self._k("sent")) or {}
            if sent.get("uid") and self.me and sent["uid"] == self.me.get("id"):
                self.sent_ids = set(int(x) for x in sent.get("ids") or [])
                if self.sent_ids:
                    self.note("ok", f"DM history loaded — {len(self.sent_ids)} already DMed, they'll be skipped.")
        except Exception:
            self.sent_ids = set()
        try:
            d = await store.get(self._k("dir")) or {}
            if d.get("uid") and self.me and d["uid"] == self.me.get("id"):
                self.dmdir = {int(k): v for k, v in (d.get("dir") or {}).items()}
                if self.dmdir:
                    self.note("ok", f"Broadcast list loaded — {len(self.dmdir)} known DM recipients.")
        except Exception:
            self.dmdir = {}
        try:
            auto = await store.get(self._k("auto")) or {}
            self.auto = auto if isinstance(auto, dict) else {}
        except Exception:
            self.auto = {}
        if self.auto.get("on"):
            self._ensure_auto_loop()
            self.note("ok", f"Auto-send restored — {len(self._auto_remaining())} still to DM.")

    def _persist_armed(self) -> None:
        store.push_soon(self._k("armed_uid"), (self.me or {}).get("id"))
        store.push_soon(self._k("armed"), [asdict(a) for a in self.armed])

    def _persist_sent(self) -> None:
        uid = (self.me or {}).get("id")
        if uid:
            store.push_soon(self._k("sent"), {"uid": uid, "ids": list(self.sent_ids)})

    def _persist_dir(self) -> None:
        store.push_soon(self._k("dir"), {"uid": (self.me or {}).get("id"), "dir": self.dmdir})

    def merge_people(self, people: list[dict[str, Any]]) -> int:
        """Remember requesters (with access hashes) as potential DM recipients."""
        n = 0
        for p in people or []:
            try:
                uid = int(p.get("id") or 0)
                ah = int(p.get("access_hash") or 0)
            except (TypeError, ValueError):
                continue
            if uid and ah:
                self.dmdir[uid] = {
                    "id": uid,
                    "access_hash": str(ah),
                    "name": p.get("name") or "",
                    "username": p.get("username"),
                }
                n += 1
        if n:
            self._persist_dir()
        return n

    def cancel_job(self) -> bool:
        j = self.job
        if j and j.status == "running":
            j.cancel_requested = True
            j.detail = "Cancelling…"
            self.emit("job", job=j.as_dict())
            self.note("ok", "Cancel requested — stopping after in-flight requests.")
            return True
        return False

    async def clear_sent(self) -> int:
        n = len(self.sent_ids)
        self.sent_ids = set()
        store.push_soon(self._k("sent"), None)
        self.note("ok", f"Cleared DM history ({n} records). Those people can be DMed again.")
        return n

    # ── auto-send ───────────────────────────────────────────
    def auto_status(self) -> dict[str, Any]:
        a = self.auto or {}
        on = bool(a.get("on"))
        return {
            "on": on,
            "interval": int(a.get("interval", 30) or 30),
            "remaining": len(self._auto_remaining()) if on else 0,
            "next_in": max(0, int((a.get("next") or 0) - time.time())) if on else 0,
        }

    def _auto_remaining(self) -> list[dict[str, Any]]:
        return [
            p
            for p in (self.auto or {}).get("people", [])
            if int(p.get("id") or 0) not in self.sent_ids
            and int(p.get("access_hash") or 0)
        ]

    def _ensure_auto_loop(self) -> None:
        if self._auto_task is None or self._auto_task.done():
            self._auto_task = asyncio.create_task(self._auto_loop(), name="hydra-auto-send")

    def start_background(self) -> None:
        if (self.auto or {}).get("on"):
            self._ensure_auto_loop()
        if self._keep_task is None or self._keep_task.done():
            self._keep_task = asyncio.create_task(self._keepalive(), name="hydra-keepalive")

    async def _keepalive(self) -> None:
        """Ping Telegram so idle sockets (Render, NAT) don't die unnoticed."""
        while True:
            await asyncio.sleep(45)
            if self.phase != "ready" or not self.client:
                continue
            try:
                if not self.client.is_connected():
                    await self._ensure()
                else:
                    await self.client.get_me()
            except Exception:
                try:
                    await self._ensure(force=True)
                except Exception:
                    pass

    async def auto_start(
        self,
        chat_id: int,
        people: list[dict[str, Any]],
        message: str,
        interval: Optional[int] = None,
        title: Optional[str] = None,
    ) -> None:
        interval = int(interval or (self.auto or {}).get("interval") or 30)
        self.auto = {
            "on": True,
            "chat_id": int(chat_id),
            "title": title or "",
            "people": list(people),
            "message": message,
            "interval": interval,
            "next": time.time() + 10,
            "passes": 0,
            "sent_total": 0,
            "last_result": "",
        }
        store.push_soon(self._k("auto"), self.auto)
        self._ensure_auto_loop()
        self.note(
            "ok",
            f"Auto-send ON — {len(self._auto_remaining())} to DM, one pass every {interval}s.",
        )
        self.emit("status", **self.snapshot())

    async def auto_stop(self) -> None:
        self.auto = {"on": False, "interval": (self.auto or {}).get("interval", 30)}
        store.push_soon(self._k("auto"), self.auto)
        self.note("ok", "Auto-send stopped.")
        self.emit("status", **self.snapshot())

    def auto_set_interval(self, seconds: int) -> None:
        a = dict(self.auto or {})
        a["interval"] = max(3, min(120, int(seconds)))
        a.setdefault("on", False)
        self.auto = a
        store.push_soon(self._k("auto"), a)

    async def _auto_loop(self) -> None:
        while True:
            await asyncio.sleep(3)
            try:
                await self._auto_tick()
            except Exception as exc:
                self.note("warn", f"Auto-send check failed: {_err_name(exc)}")

    async def _auto_tick(self, force: bool = False) -> None:
        a = self.auto or {}
        if not a.get("on") or self.phase != "ready":
            return
        # A job left "running" by a crash used to block every future pass —
        # recover instead: if it went quiet minutes ago, close it and go on.
        j = self.job
        if j and j.status == "running":
            if time.time() - getattr(j, "touched", 0) > 120:
                j.status = "done"
                j.detail = "Recovered an interrupted job — auto-send continues."
                self.note("warn", j.detail)
                self.emit("job", job=j.as_dict())
            else:
                return  # genuinely still sending
        if not force and time.time() < (a.get("next") or 0):
            return
        remaining = self._auto_remaining()
        if not remaining:
            self.auto = {"on": False, "interval": a.get("interval", 30)}
            store.push_soon(self._k("auto"), self.auto)
            self.note("ok", "Auto-send finished — everyone in the selection has been DMed. ✅")
            self.emit("status", **self.snapshot())
            return
        self.note("ok", f"Auto-send pass {int(a.get('passes', 0)) + 1}: {len(remaining)} unsent, sending.")
        passes = int(a.get("passes", 0)) + 1
        sent_total = int(a.get("sent_total", 0))
        try:
            result = await self.send_now(int(a["chat_id"]), remaining, a["message"])
            jinfo = result.get("job") or {}
            ok = int(jinfo.get("ok") or 0)
            sent_total += ok
            last = f"+{ok} sent · {int(jinfo.get('fail') or 0)} failed"
        except Exception as exc:
            if "No one left" in str(exc):
                self.auto = {**a, "on": False}
                store.push_soon(self._k("auto"), self.auto)
                self.note(
                    "ok", "Auto-send finished — the rest can't be DMed (unreachable)."
                )
                self.emit("status", **self.snapshot())
                return
            last = f"pass failed: {_err_name(exc)}"
            self.note("warn", f"Auto-send pass: {_err_name(exc)}")
        a = dict(a)
        a.update(
            {
                "passes": passes,
                "sent_total": sent_total,
                "last_result": last,
                "next": time.time() + int(a.get("interval", 30) or 30),
            }
        )
        self.auto = a
        store.push_soon(self._k("auto"), a)
        self.emit("status", **self.snapshot())

    async def auto_force_pass(self) -> None:
        """Run an auto-send pass immediately (Auto progress → Send a pass now)."""
        if (self.auto or {}).get("on"):
            await self._auto_tick(force=True)

    # ── auth ────────────────────────────────────────────────
    async def start_login(self, api_id: int, api_hash: str, phone: str) -> dict[str, Any]:
        async with self._lock:
            await self._drop_client()
            self.api_id = int(api_id)
            self.api_hash = api_hash.strip()
            self.phone = phone.strip()
            self._write_creds()
            self.client = self._make_client(StringSession())
            await self.client.connect()
            sent = await self.client.send_code_request(self.phone)
            self.phone_code_hash = sent.phone_code_hash
            self.phase = "awaiting_code"
            self.note("ok", f"Login code sent to {self.phone}.")
            self.emit("status", **self.snapshot())
            return {"phase": self.phase}

    async def submit_code(self, code: str) -> dict[str, Any]:
        if not self.client or self.phase not in ("awaiting_code", "awaiting_password"):
            raise RuntimeError("No login in progress.")
        try:
            await self.client.sign_in(
                phone=self.phone,
                code=code.strip(),
                phone_code_hash=self.phone_code_hash,
            )
        except SessionPasswordNeededError:
            self.phase = "awaiting_password"
            self.note("ok", "Two-step password required.")
            self.emit("status", **self.snapshot())
            return {"phase": self.phase}
        except PhoneCodeInvalidError:
            raise RuntimeError("That code is invalid.")
        except PhoneCodeExpiredError:
            self.phase = "logged_out"
            raise RuntimeError("That code expired. Request a new one.")
        await self._after_sign_in()
        return {"phase": self.phase, "me": self.me}

    async def submit_password(self, password: str) -> dict[str, Any]:
        if not self.client or self.phase != "awaiting_password":
            raise RuntimeError("Password not expected.")
        await self.client.sign_in(password=password)
        await self._after_sign_in()
        return {"phase": self.phase, "me": self.me}

    async def login_string(self, api_id: int, api_hash: str, session_string: str) -> dict[str, Any]:
        async with self._lock:
            await self._drop_client()
            self.api_id = int(api_id)
            self.api_hash = api_hash.strip()
            self._write_creds()
            self.client = self._make_client(StringSession(session_string.strip()))
            await self.client.connect()
            if not await self.client.is_user_authorized():
                await self._drop_client()
                raise RuntimeError("Session string is not authorized.")
            await self._after_sign_in()
            return {"phase": self.phase, "me": self.me}

    async def _after_sign_in(self) -> None:
        self._write_session_string()
        # Mirror session + creds to the state store so restarts can resume.
        if store.enabled() and self.client:
            store.push_soon(
                "creds",
                {"api_id": self.api_id, "api_hash": self.api_hash, "phone": self.phone},
            )
            store.push_soon(self._k("session"), self.client.session.save())
        await self._mark_online()
        await self._restore_state()
        self.start_background()
        self.note("ok", f"Session live as {self.me.get('name') if self.me else '?'}.")
        self.emit("status", **self.snapshot())

    async def _mark_online(self) -> None:
        assert self.client
        user = await self.client.get_me()
        self.me = {
            "id": user.id,
            "name": _display_name(user),
            "username": user.username,
            "phone": user.phone,
        }
        self.phase = "ready"

    async def logout(self) -> None:
        await self._drop_client()
        self.me = None
        self.phase = "logged_out"
        self.armed.clear()
        if self.session_path.exists():
            self.session_path.unlink()
        store.push_soon(self._k("session"), None)
        store.push_soon(self._k("creds"), None)
        store.push_soon(self._k("armed"), None)
        self.armed = []
        self.note("ok", "Session closed.")
        self.emit("status", **self.snapshot())

    async def _drop_client(self) -> None:
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass
        self.client = None
        self.phase = "logged_out"

    def _client_live(self) -> bool:
        c = self.client
        return bool(c is not None and self.phase == "ready" and c.is_connected())

    def _need(self) -> TelegramClient:
        if not self.client or self.phase != "ready":
            raise RuntimeError("Session is not logged in.")
        return self.client

    async def _ensure(self, *, force: bool = False) -> TelegramClient:
        """Return a live Telegram client, reconnecting the saved session if the socket dropped.

        HYDRA can still think you are logged in (phase=ready) after Render / NAT
        kills the TCP link. Reload chats then raises
        'Cannot send requests while disconnected' — this heals that.
        """
        async with self._conn_lock:
            if not force and self._client_live():
                assert self.client is not None
                return self.client
            await self._reconnect_unlocked(force=force)
            if not self._client_live():
                raise RuntimeError("Session is not logged in.")
            assert self.client is not None
            return self.client

    async def _reconnect_unlocked(self, *, force: bool = False) -> None:
        session_str: Optional[str] = None
        if self.client:
            try:
                session_str = self.client.session.save()
            except Exception:
                pass
            if not force and not self.client.is_connected():
                self.note("warn", "Telegram connection dropped — reconnecting…")
                try:
                    await self.client.connect()
                    if self.client.is_connected() and await self.client.is_user_authorized():
                        self.phase = "ready"
                        self.note("ok", "Reconnected to Telegram.")
                        return
                except Exception as exc:
                    self.note("warn", f"Quick reconnect failed: {_err_name(exc)}")
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.client = None

        if not session_str and self.session_path.exists():
            try:
                session_str = self.session_path.read_text().strip()
            except Exception:
                pass
        if (not self.api_id or not self.api_hash) and self.creds_path.exists():
            try:
                creds = json.loads(self.creds_path.read_text())
                self.api_id = int(creds["api_id"])
                self.api_hash = creds["api_hash"]
                self.phone = creds.get("phone") or self.phone
            except Exception:
                pass

        if not session_str or not self.api_id or not self.api_hash:
            ok = await self.try_resume()
            if ok:
                return
            self.phase = "logged_out"
            return

        try:
            self.client = self._make_client(StringSession(session_str))
            await self.client.connect()
            if not await self.client.is_user_authorized():
                await self.client.disconnect()
                self.client = None
                self.phase = "logged_out"
                self.note("warn", "Session is no longer authorized — log in again.")
                return
            await self._mark_online()
            self.note("ok", "Reconnected to Telegram.")
        except Exception as exc:
            if self.client:
                try:
                    await self.client.disconnect()
                except Exception:
                    pass
            self.client = None
            self.note("warn", f"Reconnect failed: {_err_name(exc)}")

    # ── chats & join requests ───────────────────────────────
    CHAT_CACHE_TTL = 300  # full dialog scans are heavy — cache results 5 min

    async def list_chats(self) -> list[dict[str, Any]]:
        """Chats where this account is admin (the only ones with join requests).

        A full scan walks the whole dialog list (the account may have
        thousands of dialogs with admin chats buried deep), so results are
        cached and concurrent reloads share a single scan.
        """
        if self._chats_cache is not None and time.time() - self._chats_ts < self.CHAT_CACHE_TTL:
            return self._chats_cache
        async with self._chats_lock:  # single-flight: parallel taps share one scan
            if self._chats_cache is not None and time.time() - self._chats_ts < self.CHAT_CACHE_TTL:
                return self._chats_cache
            last_exc: Optional[BaseException] = None
            for attempt in range(2):
                try:
                    client = await self._ensure(force=attempt > 0)
                    out, complete = await self._fetch_admin_chats(client)
                    self._chats_cache = out
                    # Partial scans refresh sooner so a throttled crawl can continue.
                    self._chats_ts = (
                        time.time() - (self.CHAT_CACHE_TTL - 60) if not complete else time.time()
                    )
                    return out
                except FloodWaitError as exc:
                    wait = int(getattr(exc, "seconds", 20) or 20)
                    if self._chats_cache is not None:
                        self.note(
                            "warn",
                            f"Telegram limited the chat reload ({wait}s) — showing the saved list.",
                        )
                        return self._chats_cache
                    raise RuntimeError(
                        f"Telegram asks to wait ~{wait}s before loading chats. "
                        "Try again shortly — this eases off on its own."
                    ) from exc
                except Exception as exc:
                    last_exc = exc
                    if attempt == 0 and _is_disconnect(exc):
                        self.note(
                            "warn",
                            "Telegram was disconnected — reconnecting and retrying Reload.",
                        )
                        continue
                    raise
            raise RuntimeError(str(last_exc) if last_exc else "Could not load chats.")

    async def _fetch_admin_chats(self, client: TelegramClient) -> tuple[list[dict[str, Any]], bool]:
        out: list[dict[str, Any]] = []
        seen: set[int] = set()
        self.note("ok", "Scanning your chats for admin rights… (one-time, then cached)")
        complete = True
        try:
            async for dialog in client.iter_dialogs(limit=4000):
                entity = dialog.entity
                if isinstance(entity, Channel):
                    admin = bool(entity.creator or entity.admin_rights)
                    kind = "channel" if entity.broadcast else "supergroup"
                elif isinstance(entity, Chat):
                    admin = bool(
                        getattr(entity, "creator", False) or getattr(entity, "admin_rights", None)
                    )
                    kind = "group"
                else:
                    continue
                if not admin or dialog.id in seen:
                    continue
                seen.add(dialog.id)
                out.append(
                    {
                        "id": dialog.id,
                        "title": dialog.name,
                        "kind": kind,
                        "username": getattr(entity, "username", None),
                        "admin": True,
                        "unread": dialog.unread_count,
                        "pending": None,
                    }
                )
        except FloodWaitError:
            complete = False
            self.note(
                "warn",
                f"Chat scan throttled by Telegram after {len(out)} admin chats — "
                "tap Reload in a minute to continue the scan.",
            )
        # Public channels where the account is admin but that sat outside the
        # dialog window.
        try:
            res = await client(GetAdminedPublicChannelsRequest())
            for ch in res.chats:
                if ch.id in seen:
                    continue
                seen.add(ch.id)
                out.append(
                    {
                        "id": ch.id,
                        "title": ch.title,
                        "kind": "channel" if getattr(ch, "broadcast", False) else "supergroup",
                        "username": getattr(ch, "username", None),
                        "admin": True,
                        "unread": 0,
                        "pending": None,
                    }
                )
        except Exception:
            pass
        out.sort(key=lambda c: (not c["admin"], c["title"].lower()))
        if not out:
            self.note(
                "warn",
                "No admin chats found — join requests need the session to be admin.",
            )
        return out, complete

    async def scan_pending(self, chats: list[dict[str, Any]]) -> list[dict[str, Any]]:
        client = await self._ensure()
        admin_chats = [c for c in chats if c.get("admin")]
        self.note("ok", f"Scanning {len(admin_chats)} admin chats for join requests.")

        async def one(chat: dict[str, Any]) -> None:
            try:
                peer = await client.get_input_entity(chat["id"])
                result = await client(
                    GetChatInviteImportersRequest(
                        peer=peer,
                        requested=True,
                        offset_date=EPOCH,
                        offset_user=InputUserEmpty(),
                        limit=1,
                    )
                )
                chat["pending"] = int(result.count or 0)
            except (ChatAdminRequiredError, Exception) as exc:
                chat["pending"] = None
                chat["pending_error"] = _err_name(exc)

        # Modest fan-out so the scan itself does not trip flood waits.
        sem = asyncio.Semaphore(4)

        async def wrapped(chat: dict[str, Any]) -> None:
            async with sem:
                await one(chat)

        await asyncio.gather(*(wrapped(c) for c in admin_chats))
        self.emit("chats", chats=chats)
        return chats

    async def list_requests(self, chat_id: int) -> list[dict[str, Any]]:
        client = await self._ensure()
        peer = await client.get_input_entity(chat_id)
        people: list[dict[str, Any]] = []
        offset_date: Any = EPOCH
        offset_user: Any = InputUserEmpty()
        seen: set[int] = set()
        pages = 0

        while True:
            try:
                result = await client(
                    GetChatInviteImportersRequest(
                        peer=peer,
                        requested=True,
                        offset_date=offset_date,
                        offset_user=offset_user,
                        limit=100,
                    )
                )
            except FloodWaitError as exc:
                wait = int(getattr(exc, "seconds", 1) or 1)
                self.note(
                    "warn",
                    f"Telegram throttled the listing at {len(people)} (flood {wait}s) — "
                    "already-loaded people are kept; tap Reload to continue.",
                )
                break
            except Exception as exc:
                if _is_disconnect(exc):
                    self.note("warn", "Disconnected while listing requests — reconnecting.")
                    client = await self._ensure(force=True)
                    continue
                raise
            users = {u.id: u for u in result.users if isinstance(u, User)}
            if not result.importers:
                break
            new_seen = 0
            for imp in result.importers:
                if imp.user_id in seen:
                    continue
                seen.add(imp.user_id)
                new_seen += 1
                user = users.get(imp.user_id)
                date = imp.date
                if isinstance(date, datetime):
                    date_s = date.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
                else:
                    date_s = str(date)
                people.append(
                    {
                        "id": imp.user_id,
                        "access_hash": str(int(getattr(user, "access_hash", 0) or 0)),
                        "name": _display_name(user, imp.user_id),
                        "username": getattr(user, "username", None) if user else None,
                        "about": imp.about or "",
                        "date": date_s,
                        "dm_sent": imp.user_id in self.sent_ids,
                    }
                )
            pages += 1
            if pages >= 1500:  # 150k requesters — hard safety stop
                self.note("warn", "Listing stopped at the 150k safety cap.")
                break
            if new_seen == 0:
                break  # offset no longer advancing — done
            last = result.importers[-1]
            last_user = users.get(last.user_id)
            if last_user:
                offset_date = last.date
                offset_user = InputUser(user_id=last_user.id, access_hash=last_user.access_hash)
            else:
                break
            # NOTE: do NOT stop on short pages (<100) — Telegram routinely
            # returns a few less than requested; only an empty/non-advancing
            # page means we are done. Stopping early truncated lists at ~199.
            if pages % 25 == 0:
                self.note("ok", f"Loading requests… {len(people)} so far")

        self.note("ok", f"Loaded {len(people)} pending join requests (not approved, not declined).")
        return people

    # ── burst invoke ────────────────────────────────────────
    async def _burst(
        self,
        requests: list[Any],
        job: Job,
        label: str,
        user_ids: list[int],
        commit: Optional[Callable[[list[tuple[str, Any]]], None]] = None,
        burst: Optional[int] = None,
        pace: Optional[float] = None,
    ) -> list[tuple[str, Any]]:
        """Issue many TL requests in MTProto containers instead of a slow serial loop.

        `commit`, if given, is called with the live results list after every
        container so callers can persist progress mid-job (crash-safe)."""
        client = await self._ensure()
        results: list[tuple[str, Any]] = [("pending", None)] * len(requests)
        chunk_size = int(burst or BURST)
        pace_s = float(pace if pace is not None else BURST_PACE)

        aborted: Optional[str] = None

        for start in range(0, len(requests), chunk_size):
            if job.cancel_requested:
                aborted = "Cancelled — the rest are kept for retry."
                break
            job.touched = time.time()
            chunk = requests[start : start + chunk_size]
            ids = user_ids[start : start + chunk_size]
            job.detail = f"{label} · container {start // chunk_size + 1}/{(len(requests) - 1) // chunk_size + 1}"
            self.emit("job", job=job.as_dict())

            # Indices into `chunk` not yet resolved (ok / err / skipped).
            pending = list(range(len(chunk)))
            tries = 0
            while pending and not aborted and not job.cancel_requested:
                tries += 1
                if tries > 6:
                    break
                try:
                    raw = await client([chunk[i] for i in pending])
                    if not isinstance(raw, list):
                        raw = [raw]
                    for k, item in zip(pending, raw):
                        results[start + k] = ("ok", item)
                        job.ok += 1
                    pending = []
                except MultiError as exc:
                    for i, err in enumerate(exc.exceptions):
                        k = pending[i] if i < len(pending) else None
                        if k is None:
                            continue
                        if err is None:
                            results[start + k] = ("ok", exc.results[i])
                            job.ok += 1
                        else:
                            results[start + k] = ("err", err)
                            job.fail += 1
                            job.errors.append({"user_id": ids[k], "error": _err_name(err)})
                    pending = []
                except FloodWaitError as exc:
                    # Whole container flood-limited: fail-fast, no waiting.
                    wait = int(getattr(exc, "seconds", 1) or 1)
                    for k in pending:
                        results[start + k] = ("err", exc)
                        job.fail += 1
                        job.errors.append({"user_id": ids[k], "error": f"flood_wait:{wait}"})
                    pending = []
                except Exception as exc:
                    if _is_disconnect(exc):
                        self.note("warn", "Telegram disconnected mid-job — reconnecting.")
                        try:
                            client = await self._ensure(force=True)
                            continue
                        except Exception as exc2:
                            self.note("warn", f"Reconnect failed: {_err_name(exc2)}")
                    self.note("warn", f"Container failed ({_err_name(exc)}); retrying peers one by one.")
                    for k in pending:
                        try:
                            item = await client(chunk[k])
                            results[start + k] = ("ok", item)
                            job.ok += 1
                        except FloodWaitError as fw:
                            results[start + k] = ("err", fw)
                            job.fail += 1
                            job.errors.append(
                                {
                                    "user_id": ids[k],
                                    "error": f"flood_wait:{int(getattr(fw, 'seconds', 1) or 1)}",
                                }
                            )
                        except Exception as exc2:
                            results[start + k] = ("err", exc2)
                            job.fail += 1
                            job.errors.append({"user_id": ids[k], "error": _err_name(exc2)})
                    pending = []

            if job.cancel_requested and not aborted:
                aborted = "Cancelled — the rest are kept for retry."
            if aborted:
                break
            job.done = min(start + len(chunk), len(requests))
            self.emit("job", job=job.as_dict())
            if commit:
                try:
                    commit(results)
                except Exception:
                    pass
            if start + chunk_size < len(requests):
                await asyncio.sleep(pace_s)

        # Anything never attempted is "skipped", not failed — callers keep them
        # armed / know they can be retried once Telegram relaxes the limit.
        skipped = 0
        for i, (status, _) in enumerate(results):
            if status == "pending":
                results[i] = ("skip", None)
                skipped += 1
        job.skipped = skipped
        if job.cancel_requested:
            job.status = "cancelled"
        if aborted:
            job.detail = f"{aborted} ({skipped} skipped)"
            self.note("warn", job.detail)
        self.emit("job", job=job.as_dict())
        if commit:
            try:
                commit(results)
            except Exception:
                pass
        return results

    def _targets_from_people(
        self, people: list[dict[str, Any]], message: str, chat_id: int
    ) -> tuple[list[ArmedTarget], int]:
        text = message.strip()
        if not text:
            raise RuntimeError("Message is empty.")
        if not people:
            raise RuntimeError("No requesters selected.")
        targets: list[ArmedTarget] = []
        missing_hash = 0
        already_sent = 0
        for p in people:
            ah = int(p.get("access_hash") or 0)
            if not ah:
                missing_hash += 1
                continue
            if int(p["id"]) in self.sent_ids:
                already_sent += 1
                continue
            targets.append(
                ArmedTarget(
                    user_id=int(p["id"]),
                    access_hash=ah,
                    name=p.get("name") or "",
                    username=p.get("username"),
                    message=text,
                    from_chat_id=int(chat_id),
                )
            )
        if missing_hash:
            self.note("warn", f"Skipped {missing_hash} requester(s) with no access_hash.")
        if already_sent:
            self.note("ok", f"{already_sent} requester(s) already DMed before — excluded, no double DMs.")
        if not targets:
            raise RuntimeError("No one left to DM — selection is empty, already DMed, or unreachable.")
        return targets, already_sent

    def _peer(self, t: ArmedTarget) -> InputPeerUser:
        return InputPeerUser(user_id=t.user_id, access_hash=t.access_hash)

    # ── arm / fire / send ───────────────────────────────────
    async def arm_drafts(
        self, chat_id: int, people: list[dict[str, Any]], message: str
    ) -> dict[str, Any]:
        """Write the same message as an unsent draft in each requester's DM, in bursts."""
        async with self._job_lock:
            targets, already_sent = self._targets_from_people(people, message, chat_id)
            self.merge_people(people)

            # Resume: anyone who already has a draft written is skipped, so a
            # stopped run continues where it stopped. The armed list only
            # empties via Send all drafts (sent) or Clear drafts.
            armed_ids = {a.user_id for a in self.armed}
            target_ids = {t.user_id for t in targets}
            have_draft = sum(1 for t in targets if t.user_id in armed_ids)
            stale_msg = sum(
                1
                for a in self.armed
                if a.user_id in target_ids and a.message.strip() != message.strip()
            )
            base_armed = [a for a in self.armed if a.user_id not in target_ids]
            targets = [t for t in targets if t.user_id not in armed_ids]
            if have_draft:
                self.note(
                    "ok",
                    f"{have_draft} draft(s) already written — continuing from where you stopped.",
                )
                if stale_msg:
                    self.note(
                        "warn",
                        f"{stale_msg} armed draft(s) keep their earlier text — "
                        "Clear drafts to rewrite everyone.",
                    )
            if not targets:
                job = Job(id=f"arm-{int(time.time())}", kind="arm", total=have_draft)
                job.status = "done"
                job.ok = have_draft
                job.detail = f"Nothing new to write — all {have_draft} selected people already have drafts."
                self.emit("job", job=job.as_dict())
                self.emit("status", **self.snapshot())
                self.note("ok", job.detail)
                return {"job": job.as_dict(), "armed": len(self.armed)}

            job = Job(id=f"arm-{int(time.time())}", kind="arm", total=len(targets))
            self.job = job
            self.emit("job", job=job.as_dict())
            self.note("ok", f"Writing {len(targets)} drafts ({have_draft} already done).")

            requests = [
                SaveDraftRequest(peer=self._peer(t), message=t.message, no_webpage=True)
                for t in targets
            ]

            def commit(results: list[tuple[str, Any]]) -> None:
                # Persist after every container: a restart/cancel mid-job
                # never loses the drafts already written.
                written = [t for t, (s, _) in zip(targets, results) if s == "ok"]
                self.armed = base_armed + written
                self._persist_armed()

            results = await self._burst(
                requests,
                job,
                "arm drafts",
                [t.user_id for t in targets],
                commit=commit,
                burst=ARM_BURST,
                pace=ARM_PACE,
            )

            armed: list[ArmedTarget] = []
            for t, (status, _) in zip(targets, results):
                if status == "ok":
                    armed.append(t)
            # Replace previous arm for these users, keep others.
            self.armed = base_armed + armed
            self._persist_armed()

            job.status = "done" if job.status != "cancelled" else job.status
            if job.status == "cancelled":
                job.detail = (
                    f"Cancelled · {job.ok} written just now · {job.skipped} left — "
                    "Write drafts continues from here"
                )
            else:
                job.detail = f"Drafts written in {job.ok + have_draft} DMs (unsent)"
                if have_draft:
                    job.detail += f" ({have_draft} were already done)"
                if already_sent:
                    job.detail += f" · {already_sent} already DMed, excluded"
                if job.skipped:
                    job.detail += f" · {job.skipped} skipped (flood — try again later)"
            self.emit("job", job=job.as_dict())
            self.emit("status", **self.snapshot())
            self.note("ok", job.detail)
            return {"job": job.as_dict(), "armed": len(self.armed)}

    async def fire_drafts(self) -> dict[str, Any]:
        """Send every armed draft in burst containers. One click."""
        async with self._job_lock:
            if not self.armed:
                raise RuntimeError("Nothing is armed. Write drafts first.")
            client = await self._ensure()
            targets = list(self.armed)
            job = Job(id=f"fire-{int(time.time())}", kind="fire", total=len(targets))
            self.job = job
            self.emit("job", job=job.as_dict())
            self.note("ok", f"Firing {len(targets)} prepared DMs.")

            # Prefer the text actually sitting in the account drafts.
            draft_text: dict[int, str] = {}
            drafts_read = False
            try:
                updates = await client(GetAllDraftsRequest())
                drafts_read = True
                for upd in getattr(updates, "updates", []) or []:
                    if not isinstance(upd, UpdateDraftMessage):
                        continue
                    peer = upd.peer
                    draft = upd.draft
                    if isinstance(peer, PeerUser) and isinstance(draft, DraftMessage) and draft.message:
                        draft_text[peer.user_id] = draft.message
            except Exception as exc:
                self.note("warn", f"Could not read live drafts, using armed copy: {_err_name(exc)}")

            if drafts_read:
                # A target whose live draft vanished was already sent or
                # cleared elsewhere — firing at it would duplicate the DM.
                before = len(targets)
                targets = [t for t in targets if t.user_id in draft_text]
                dropped = before - len(targets)
                if dropped:
                    self.note("ok", f"{dropped} drafts already sent or cleared — skipping them.")
                if not targets:
                    self.armed = []
                    self._persist_armed()
                    job.status = "done"
                    job.detail = "Nothing to send — every draft was already sent or cleared."
                    self.emit("job", job=job.as_dict())
                    self.emit("status", **self.snapshot())
                    return {"job": job.as_dict(), "armed": 0}

            requests = []
            for t in targets:
                text = draft_text.get(t.user_id, t.message)
                requests.append(
                    SendMessageRequest(
                        peer=self._peer(t),
                        message=text,
                        random_id=random.randrange(1, 2**63),
                        no_webpage=True,
                        clear_draft=True,
                    )
                )

            def commit(results: list[tuple[str, Any]]) -> None:
                # Persist after every container: sent users are recorded and
                # the armed list shrinks as drafts go out — crash-safe.
                ok_ids = {t.user_id for t, (s, _) in zip(targets, results) if s == "ok"}
                if ok_ids:
                    self.sent_ids.update(ok_ids)
                    self._persist_sent()
                self.armed = [t for t, (s, _) in zip(targets, results) if s != "ok"]
                self._persist_armed()

            results = await self._burst(
                requests, job, "fire drafts", [t.user_id for t in targets], commit=commit
            )

            # Keep every target that did not go out (failed or flood-skipped)
            # armed, so "Send all drafts" can resume them later.
            self.armed = [t for t, (status, _) in zip(targets, results) if status != "ok"]
            self._persist_armed()
            self.merge_people(
                [
                    {
                        "id": t.user_id,
                        "access_hash": str(t.access_hash),
                        "name": t.name,
                        "username": t.username,
                    }
                    for t in targets
                ]
            )
            fired = [t.user_id for t, (status, _) in zip(targets, results) if status == "ok"]
            if fired:
                self.sent_ids.update(fired)
                self._persist_sent()

            job.status = "done" if job.status != "cancelled" else job.status
            if job.status == "cancelled":
                job.detail = f"Cancelled · sent {job.ok} · {job.skipped} still armed"
            else:
                job.detail = f"Sent {job.ok} · failed {job.fail}"
                if job.skipped:
                    job.detail += f" · {job.skipped} skipped (flood — still armed)"
            self.emit("job", job=job.as_dict())
            self.emit("status", **self.snapshot())
            self.note("ok", job.detail)
            return {"job": job.as_dict(), "armed": len(self.armed)}

    async def send_now(
        self, chat_id: int, people: list[dict[str, Any]], message: str
    ) -> dict[str, Any]:
        async with self._job_lock:
            targets, already_sent = self._targets_from_people(people, message, chat_id)
            self.merge_people(people)
            job = Job(id=f"send-{int(time.time())}", kind="send", total=len(targets))
            self.job = job
            self.emit("job", job=job.as_dict())
            self.note("ok", f"Sending {len(targets)} DMs now.")
            requests = [
                SendMessageRequest(
                    peer=self._peer(t),
                    message=t.message,
                    random_id=random.randrange(1, 2**63),
                    no_webpage=True,
                    clear_draft=True,
                )
                for t in targets
            ]
            def commit(results: list[tuple[str, Any]]) -> None:
                ok_ids = {t.user_id for t, (s, _) in zip(targets, results) if s == "ok"}
                if ok_ids:
                    self.sent_ids.update(ok_ids)
                    self._persist_sent()

            results = await self._burst(
                requests, job, "send now", [t.user_id for t in targets], commit=commit
            )
            for t, (status, _) in zip(targets, results):
                if status == "ok":
                    self.sent_ids.add(t.user_id)
            self._persist_sent()
            job.status = "done" if job.status != "cancelled" else job.status
            if job.status == "cancelled":
                job.detail = f"Cancelled · sent {job.ok} · {job.skipped} not sent"
            else:
                job.detail = f"Sent {job.ok} · failed {job.fail}"
                if already_sent:
                    job.detail += f" · {already_sent} already DMed, excluded"
                if job.skipped:
                    job.detail += f" · {job.skipped} skipped (flood) — send to them again later"
                    self.note(
                        "warn",
                        f"{job.skipped} DMs skipped: Telegram limits DMs to strangers. "
                        "Try again in a few hours.",
                    )
            self.emit("job", job=job.as_dict())
            self.note("ok", job.detail)
            return {"job": job.as_dict()}

    async def broadcast(self, message: str) -> dict[str, Any]:
        """Send one message to EVERY known DM recipient (people DMed before).

        Unlike Send DMs now, broadcast deliberately re-DMs people — that is
        its purpose. Respects the same flood handling and cancel button.
        """
        async with self._job_lock:
            text = message.strip()
            if not text:
                raise RuntimeError("Message is empty. Set it on the Message screen.")
            await self._ensure()
            targets = [
                ArmedTarget(
                    user_id=int(uid),
                    access_hash=int(v.get("access_hash") or 0),
                    name=v.get("name") or "",
                    username=v.get("username"),
                    message=text,
                    from_chat_id=0,
                )
                for uid, v in self.dmdir.items()
                if int(v.get("access_hash") or 0)
            ]
            if not targets:
                raise RuntimeError(
                    "No known recipients yet — load requests / DM some people first."
                )
            job = Job(id=f"bcast-{int(time.time())}", kind="broadcast", total=len(targets))
            self.job = job
            self.emit("job", job=job.as_dict())
            self.note("ok", f"Broadcasting to {len(targets)} recipients.")
            requests = [
                SendMessageRequest(
                    peer=self._peer(t),
                    message=t.message,
                    random_id=random.randrange(1, 2**63),
                    no_webpage=True,
                )
                for t in targets
            ]
            def commit(results: list[tuple[str, Any]]) -> None:
                ok_ids = {t.user_id for t, (s, _) in zip(targets, results) if s == "ok"}
                if ok_ids:
                    self.sent_ids.update(ok_ids)
                    self._persist_sent()

            results = await self._burst(
                requests, job, "broadcast", [t.user_id for t in targets], commit=commit
            )
            sent_ok = [t.user_id for t, (status, _) in zip(targets, results) if status == "ok"]
            if sent_ok:
                self.sent_ids.update(sent_ok)
                self._persist_sent()
            job.status = "done" if job.status != "cancelled" else job.status
            if job.status == "cancelled":
                job.detail = f"Cancelled · sent {job.ok} · {job.skipped} not sent"
            else:
                job.detail = f"Broadcast · sent {job.ok} · failed {job.fail}"
                if job.skipped:
                    job.detail += f" · {job.skipped} skipped (flood — broadcast again later)"
            self.emit("job", job=job.as_dict())
            self.emit("status", **self.snapshot())
            self.note(job.status == "cancelled" and "warn" or "ok", job.detail)
            return {"job": job.as_dict(), "recipients": len(targets)}

    async def disarm(self) -> dict[str, Any]:
        async with self._job_lock:
            if not self.armed:
                return {"armed": 0}
            targets = list(self.armed)
            job = Job(id=f"disarm-{int(time.time())}", kind="disarm", total=len(targets))
            self.job = job
            self.emit("job", job=job.as_dict())
            requests = [
                SaveDraftRequest(peer=self._peer(t), message="", no_webpage=True)
                for t in targets
            ]
            await self._burst(requests, job, "clear drafts", [t.user_id for t in targets])
            self.armed = []
            self._persist_armed()
            job.status = "done"
            job.detail = "Drafts cleared"
            self.emit("job", job=job.as_dict())
            self.emit("status", **self.snapshot())
            self.note("ok", "Armed drafts cleared.")
            return {"job": job.as_dict(), "armed": 0}

    def armed_list(self) -> list[dict[str, Any]]:
        return [asdict(t) | {"message": t.message[:80]} for t in self.armed]


class EnginePool:
    """All connected account sessions. The module-level `engine` proxy
    forwards to the active one, so existing code keeps working."""

    def __init__(self) -> None:
        self.engines: dict[str, HydraEngine] = {}
        self.order: list[str] = []
        self.active_key: Optional[str] = None

    # ── roster ───────────────────────────────────────────────
    def _register(self, key: str, eng: HydraEngine, make_active: bool = True) -> None:
        self.engines[key] = eng
        if key not in self.order:
            self.order.append(key)
        if make_active or self.active_key is None:
            self.active_key = key
        self.persist_meta()

    def persist_meta(self) -> None:
        store.push_soon("pool", {"keys": list(self.order), "active": self.active_key})

    def active(self) -> HydraEngine:
        eng = self.engines.get(self.active_key or "")
        if eng is None and self.order:
            eng = self.engines[self.order[0]]
            self.active_key = self.order[0]
        if eng is None:
            eng = HydraEngine("__none__")  # blank placeholder until a session exists
        return eng

    def ensure_active(self) -> HydraEngine:
        """Active engine, creating an empty 'main' slot if the pool is empty."""
        if self.engines:
            return self.active()
        eng = HydraEngine("main")
        self._register("main", eng)
        return eng

    def summary(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for k in self.order:
            e = self.engines.get(k)
            if e is None:
                continue
            out.append(
                {
                    "key": k,
                    "active": k == self.active_key,
                    "phase": e.phase,
                    "me": e.me,
                    "armed": len(e.armed),
                    "auto": bool((e.auto or {}).get("on")),
                }
            )
        return out

    # ── lifecycle ────────────────────────────────────────────
    def new_login(self) -> HydraEngine:
        """Fresh blank engine for the login wizard; adopt() keys it after."""
        return HydraEngine("tmp" + str(int(time.time() * 1000) % 100_000_000))

    async def adopt(self, eng: HydraEngine) -> str:
        """Give a freshly-logged-in engine its final key and register it."""
        temp = eng.skey
        me = eng.me or {}
        key = "s" + str(me.get("id") or eng.phone or int(time.time()))
        if key in self.engines:
            old = self.engines.pop(key)
            if key in self.order:
                self.order.remove(key)
            try:
                await old.logout()
            except Exception:
                pass
        eng.skey = key
        eng.creds_path = DATA / f"creds-{key}.json"
        eng.session_path = DATA / f"session-{key}.string"
        eng._write_creds()
        eng._write_session_string()
        store.push_soon(eng._k("armed_uid"), (me or {}).get("id"))
        store.push_soon(eng._k("armed"), [])
        store.push_soon(eng._k("sent"), {"uid": (me or {}).get("id"), "ids": []})
        store.push_soon(eng._k("auto"), {"on": False, "interval": 30})
        # purge temp keys from the store
        if temp != key:
            for name in ("creds", "session", "armed", "armed_uid", "sent", "dir", "auto"):
                store.push_soon(f"{temp}:{name}", None)
        self._register(key, eng)
        eng.start_background()
        return key

    async def switch(self, key: str) -> bool:
        if key not in self.engines:
            return False
        self.active_key = key
        self.persist_meta()
        return True

    def forget(self, key: str) -> None:
        """Deregister without touching the (already logged-out) engine."""
        if key in self.engines:
            del self.engines[key]
        if key in self.order:
            self.order.remove(key)
        if self.active_key == key:
            self.active_key = self.order[0] if self.order else None
        self.persist_meta()

    async def remove(self, key: str) -> None:
        eng = self.engines.get(key)
        if eng is None:
            self.forget(key)
            return
        try:
            await eng.logout()
        except Exception:
            pass
        self.forget(key)

    async def _has_stored_sessions(self) -> bool:
        if not await store.ensure_ready():
            return False
        if await store.get("session"):
            return True
        meta = await store.get("pool") or {}
        for k in meta.get("keys") or []:
            if k != "main" and await store.get(f"{k}:session"):
                return True
        return False

    async def self_heal(self) -> None:
        """Watchdog: restore missing sessions AND reconnect dropped sockets.

        An engine can stay phase=ready after Telegram/NAT kills the TCP
        link; Reload chats then errors with 'Cannot send requests while
        disconnected'. Ping those engines here so they heal without a
        redeploy.
        """
        while True:
            await asyncio.sleep(45)
            try:
                if not self.engines:
                    if not await self._has_stored_sessions():
                        continue  # user genuinely has no session yet
                    await self.bootstrap()
                    if self.engines:
                        for e in self.engines.values():
                            e.start_background()
                        self.active().note(
                            "ok",
                            "Session reconnected automatically after a failed startup restore.",
                        )
                    continue
                for e in list(self.engines.values()):
                    try:
                        if e.phase == "ready" and e.client and e.client.is_connected():
                            continue
                        if e.phase == "ready" or e.session_path.exists():
                            await e._ensure()
                    except Exception:
                        pass
            except Exception:
                pass

    async def bootstrap(self) -> None:
        """Restore every stored session at boot (legacy data becomes 'main')."""
        meta = await store.get("pool") or {}
        keys = list(meta.get("keys") or [])
        main = HydraEngine("main")
        await main.try_resume()
        if main.phase == "ready":
            self._register("main", main, make_active=(meta.get("active") == "main"))
        for k in keys:
            if k == "main":
                continue
            e = HydraEngine(k)
            try:
                await e.try_resume()
            except Exception:
                pass
            if e.phase == "ready":
                self._register(k, e, make_active=(meta.get("active") == k))
        if self.active_key is None and self.order:
            self.active_key = self.order[0]
            self.persist_meta()


class _EngineProxy:
    """Forwards attribute access to the active engine."""

    def __getattr__(self, name: str) -> Any:
        return getattr(pool.active(), name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(pool.active(), name, value)


pool = EnginePool()
engine = _EngineProxy()
