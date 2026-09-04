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
from typing import Any, Optional

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
BURST = 24
# Flood-wait policy: short waits are slept off and the job resumes on its own.
# A single wait longer than FLOOD_SLEEP_CAP (or too much total waiting) pauses
# the job instead of freezing it for hours; unattempted targets are reported
# as "skipped" and (for fired drafts) stay armed so they can be retried later.
FLOOD_SLEEP_CAP = 600
FLOOD_TOTAL_SLEEP_CAP = 1200
FLOOD_MAX_TRIES = 3
BURST_PACE = 1.5
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _display_name(user: User | None, fallback_id: int = 0) -> str:
    if user is None:
        return f"user:{fallback_id}"
    parts = [user.first_name or "", user.last_name or ""]
    name = " ".join(p for p in parts if p).strip()
    return name or (f"@{user.username}" if user.username else f"user:{user.id}")


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
    def __init__(self) -> None:
        self.client: Optional[TelegramClient] = None
        self.api_id: Optional[int] = None
        self.api_hash: Optional[str] = None
        self.phone: Optional[str] = None
        self.phone_code_hash: Optional[str] = None
        self.me: Optional[dict[str, Any]] = None
        # user_ids this account has already DMed — never double-send (persisted).
        self.sent_ids: set[int] = set()
        self.phase: str = "logged_out"
        self.armed: list[ArmedTarget] = []
        self.job: Optional[Job] = None
        self.logs: deque[dict[str, Any]] = deque(maxlen=300)
        self._listeners: set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()
        self._job_lock = asyncio.Lock()

    def _make_client(self, session: StringSession) -> TelegramClient:
        client = TelegramClient(
            session,
            self.api_id,
            self.api_hash,
            device_model="HYDRA 324",
            system_version="Linux",
            app_version="0.1.0",
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
            "job": self.job.as_dict() if self.job else None,
        }

    # ── persistence ─────────────────────────────────────────
    def _write_creds(self) -> None:
        DATA.mkdir(parents=True, exist_ok=True)
        CREDS_PATH.write_text(
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
        SESSION_PATH.write_text(raw)

    def export_session_string(self) -> Optional[str]:
        if not self.client:
            return None
        return self.client.session.save()

    async def try_resume(self) -> None:
        if not CREDS_PATH.exists() or not SESSION_PATH.exists():
            # Ephemeral filesystem (e.g. Render free tier): restore from the
            # Supabase state store before giving up.
            await store.pull_to_file("creds", CREDS_PATH)
            await store.pull_to_file("session", SESSION_PATH)
        if not CREDS_PATH.exists() or not SESSION_PATH.exists():
            return
        try:
            creds = json.loads(CREDS_PATH.read_text())
            session = SESSION_PATH.read_text().strip()
            if not session or not creds.get("api_id"):
                return
            self.api_id = int(creds["api_id"])
            self.api_hash = creds["api_hash"]
            self.phone = creds.get("phone")
            self.client = self._make_client(StringSession(session))
            await self.client.connect()
            if await self.client.is_user_authorized():
                await self._mark_online()
                self.note("ok", "Resumed existing session.")
                await self._restore_state()
            else:
                await self.client.disconnect()
                self.client = None
                self.phase = "logged_out"
        except Exception as exc:
            self.note("warn", f"Could not resume session: {_err_name(exc)}")
            self.client = None
            self.phase = "logged_out"

    async def _restore_state(self) -> None:
        """Rebuild armed drafts + DM history from the state store (survives restarts)."""
        try:
            armed_uid = await store.get("armed_uid")
            if armed_uid is None or armed_uid == (self.me or {}).get("id"):
                raw = await store.get("armed") or []
                self.armed = [ArmedTarget(**a) for a in raw]
            else:
                self.armed = []  # armed list belonged to a different account
        except Exception:
            self.armed = []
        if self.armed:
            self.note("ok", f"Restored {len(self.armed)} armed drafts — Send all drafts to resume them.")
        try:
            sent = await store.get("sent") or {}
            if sent.get("uid") and self.me and sent["uid"] == self.me.get("id"):
                self.sent_ids = set(int(x) for x in sent.get("ids") or [])
                if self.sent_ids:
                    self.note("ok", f"DM history loaded — {len(self.sent_ids)} already DMed, they'll be skipped.")
        except Exception:
            self.sent_ids = set()

    def _persist_armed(self) -> None:
        store.push_soon("armed_uid", (self.me or {}).get("id"))
        store.push_soon("armed", [asdict(a) for a in self.armed])

    def _persist_sent(self) -> None:
        uid = (self.me or {}).get("id")
        if uid:
            store.push_soon("sent", {"uid": uid, "ids": list(self.sent_ids)})

    async def clear_sent(self) -> int:
        n = len(self.sent_ids)
        self.sent_ids = set()
        store.push_soon("sent", None)
        self.note("ok", f"Cleared DM history ({n} records). Those people can be DMed again.")
        return n

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
            store.push_soon("session", self.client.session.save())
        await self._mark_online()
        await self._restore_state()
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
        if SESSION_PATH.exists():
            SESSION_PATH.unlink()
        store.push_soon("session", None)
        store.push_soon("creds", None)
        store.push_soon("armed", None)
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

    def _need(self) -> TelegramClient:
        if not self.client or self.phase != "ready":
            raise RuntimeError("Session is not logged in.")
        return self.client

    # ── chats & join requests ───────────────────────────────
    async def list_chats(self) -> list[dict[str, Any]]:
        client = self._need()
        out: list[dict[str, Any]] = []
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, User):
                continue
            kind = "group"
            admin = False
            username = getattr(entity, "username", None)
            if isinstance(entity, Channel):
                kind = "channel" if entity.broadcast else "supergroup"
                admin = bool(entity.creator or entity.admin_rights)
            elif isinstance(entity, Chat):
                kind = "group"
                admin = bool(getattr(entity, "creator", False) or getattr(entity, "admin_rights", None))
            else:
                continue
            out.append(
                {
                    "id": dialog.id,
                    "title": dialog.name,
                    "kind": kind,
                    "username": username,
                    "admin": admin,
                    "unread": dialog.unread_count,
                    "pending": None,
                }
            )
        out.sort(key=lambda c: (not c["admin"], c["title"].lower()))
        return out

    async def scan_pending(self, chats: list[dict[str, Any]]) -> list[dict[str, Any]]:
        client = self._need()
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
        client = self._need()
        peer = await client.get_input_entity(chat_id)
        people: list[dict[str, Any]] = []
        offset_date: Any = EPOCH
        offset_user: Any = InputUserEmpty()
        seen: set[int] = set()
        pages = 0
        flood_budget = 300  # seconds of flood-wait tolerated while listing

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
                wait = min(int(getattr(exc, "seconds", 1) or 1), 60)
                if flood_budget <= 0:
                    self.note(
                        "warn",
                        f"Listing paused at {len(people)} — Telegram is throttling. "
                        "Open Requests again later to load the rest.",
                    )
                    break
                flood_budget -= wait
                self.note("warn", f"Flood wait {wait}s while listing requests — continuing.")
                await asyncio.sleep(wait + 1)
                continue
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
            await asyncio.sleep(0.2)

        self.note("ok", f"Loaded {len(people)} pending join requests (not approved, not declined).")
        return people

    # ── burst invoke ────────────────────────────────────────
    async def _burst(
        self,
        requests: list[Any],
        job: Job,
        label: str,
        user_ids: list[int],
    ) -> list[tuple[str, Any]]:
        """Issue many TL requests in MTProto containers instead of a slow serial loop."""
        client = self._need()
        results: list[tuple[str, Any]] = [("pending", None)] * len(requests)

        flood_sleep_total = 0
        aborted: Optional[str] = None

        for start in range(0, len(requests), BURST):
            chunk = requests[start : start + BURST]
            ids = user_ids[start : start + BURST]
            job.detail = f"{label} · container {start // BURST + 1}/{(len(requests) - 1) // BURST + 1}"
            self.emit("job", job=job.as_dict())

            # Indices into `chunk` not yet resolved (ok / err / skipped).
            pending = list(range(len(chunk)))
            tries = 0
            while pending and not aborted:
                tries += 1
                try:
                    raw = await client([chunk[i] for i in pending])
                    if not isinstance(raw, list):
                        raw = [raw]
                    for k, item in zip(pending, raw):
                        results[start + k] = ("ok", item)
                        job.ok += 1
                    pending = []
                except MultiError as exc:
                    retry: list[int] = []
                    max_wait = 0
                    for i, err in enumerate(exc.exceptions):
                        k = pending[i] if i < len(pending) else None
                        if k is None:
                            continue
                        if err is None:
                            results[start + k] = ("ok", exc.results[i])
                            job.ok += 1
                        elif isinstance(err, FloodWaitError):
                            # Per-item flood: retry just these after a pause.
                            retry.append(k)
                            max_wait = max(max_wait, int(getattr(err, "seconds", 1) or 1))
                        else:
                            results[start + k] = ("err", err)
                            job.fail += 1
                            job.errors.append({"user_id": ids[k], "error": _err_name(err)})
                    if not retry:
                        pending = []
                        break
                    if (
                        tries >= FLOOD_MAX_TRIES
                        or max_wait > FLOOD_SLEEP_CAP
                        or flood_sleep_total + max_wait > FLOOD_TOTAL_SLEEP_CAP
                    ):
                        aborted = (
                            f"Telegram demands a break ({max_wait}s+) — paused with {job.ok} done. "
                            "The rest are kept; retry in a few hours."
                        )
                        break
                    flood_sleep_total += max_wait
                    job.detail = (
                        f"Flood wait {max_wait}s on {len(retry)} items — auto-resuming, "
                        f"{job.ok}/{len(requests)} done"
                    )
                    self.emit("job", job=job.as_dict())
                    self.note("warn", f"Flood wait {max_wait}s during {label}; resuming automatically.")
                    await asyncio.sleep(max_wait + 1)
                    pending = retry
                except FloodWaitError as exc:
                    # Whole container rejected: nothing executed, safe to retry all.
                    wait = int(getattr(exc, "seconds", 1) or 1)
                    if (
                        tries >= FLOOD_MAX_TRIES
                        or wait > FLOOD_SLEEP_CAP
                        or flood_sleep_total + wait > FLOOD_TOTAL_SLEEP_CAP
                    ):
                        aborted = (
                            f"Telegram demands a break ({wait}s) — paused with {job.ok} done. "
                            "The rest are kept; retry in a few hours."
                        )
                        break
                    flood_sleep_total += wait
                    job.detail = f"Telegram flood wait {wait}s — auto-resuming, {job.ok}/{len(requests)} done"
                    self.emit("job", job=job.as_dict())
                    self.note("warn", f"Flood wait {wait}s during {label}; resuming automatically.")
                    await asyncio.sleep(wait + 1)
                except Exception as exc:
                    self.note("warn", f"Container failed ({_err_name(exc)}); retrying peers one by one.")
                    for k in pending:
                        try:
                            item = await client(chunk[k])
                            results[start + k] = ("ok", item)
                            job.ok += 1
                        except FloodWaitError as fw:
                            wait = int(getattr(fw, "seconds", 1) or 1)
                            if wait > FLOOD_SLEEP_CAP:
                                aborted = (
                                    f"Telegram demands a break ({wait}s) — paused with {job.ok} done. "
                                    "The rest are kept; retry in a few hours."
                                )
                                break
                            await asyncio.sleep(wait + 1)
                            try:
                                item = await client(chunk[k])
                                results[start + k] = ("ok", item)
                                job.ok += 1
                            except Exception as exc2:
                                results[start + k] = ("err", exc2)
                                job.fail += 1
                                job.errors.append({"user_id": ids[k], "error": _err_name(exc2)})
                        except Exception as exc2:
                            results[start + k] = ("err", exc2)
                            job.fail += 1
                            job.errors.append({"user_id": ids[k], "error": _err_name(exc2)})
                    pending = []

            if aborted:
                break
            job.done = min(start + len(chunk), len(requests))
            self.emit("job", job=job.as_dict())
            if start + BURST < len(requests):
                await asyncio.sleep(BURST_PACE)

        # Anything never attempted is "skipped", not failed — callers keep them
        # armed / know they can be retried once Telegram relaxes the limit.
        skipped = 0
        for i, (status, _) in enumerate(results):
            if status == "pending":
                results[i] = ("skip", None)
                skipped += 1
        job.skipped = skipped
        if aborted:
            job.detail = f"{aborted} ({skipped} skipped)"
            self.note("warn", job.detail)
        self.emit("job", job=job.as_dict())
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
            job = Job(id=f"arm-{int(time.time())}", kind="arm", total=len(targets))
            self.job = job
            self.emit("job", job=job.as_dict())
            self.note("ok", f"Arming {len(targets)} drafts in one burst sequence.")

            requests = [
                SaveDraftRequest(peer=self._peer(t), message=t.message, no_webpage=True)
                for t in targets
            ]
            results = await self._burst(
                requests, job, "arm drafts", [t.user_id for t in targets]
            )

            armed: list[ArmedTarget] = []
            for t, (status, _) in zip(targets, results):
                if status == "ok":
                    armed.append(t)
            # Replace previous arm for these users, keep others.
            keep_ids = {t.user_id for t in armed}
            self.armed = [a for a in self.armed if a.user_id not in keep_ids] + armed
            self._persist_armed()

            job.status = "done"
            job.detail = f"Drafts written in {job.ok} DMs (unsent)"
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
            client = self._need()
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

            results = await self._burst(requests, job, "fire drafts", [t.user_id for t in targets])

            # Keep every target that did not go out (failed or flood-skipped)
            # armed, so "Send all drafts" can resume them later.
            self.armed = [t for t, (status, _) in zip(targets, results) if status != "ok"]
            self._persist_armed()
            fired = [t.user_id for t, (status, _) in zip(targets, results) if status == "ok"]
            if fired:
                self.sent_ids.update(fired)
                self._persist_sent()

            job.status = "done"
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
            results = await self._burst(requests, job, "send now", [t.user_id for t in targets])
            for t, (status, _) in zip(targets, results):
                if status == "ok":
                    self.sent_ids.add(t.user_id)
            self._persist_sent()
            job.status = "done"
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


engine = HydraEngine()
