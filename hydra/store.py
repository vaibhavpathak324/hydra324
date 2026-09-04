"""Supabase-backed state store.

Render's free tier has an ephemeral filesystem: anything written to data/ is
lost on every deploy or restart. When DATABASE_URL is set (Supabase session
pooler recommended), HYDRA mirrors its small state files (Telegram session
string, api creds, control-bot token/owner) into a tiny key/value table so the
deployment resumes cleanly after restarts.

The store is best-effort: if the database is unreachable the app keeps working
exactly like before, just without durable persistence.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("hydra.store")

DSN = (os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL") or "").strip()

_pool: Any = None
_ready = False

# NOTE: defined before `async def set(...)` below, which shadows the builtin.
_TASKS: "set[asyncio.Task]" = set()


def enabled() -> bool:
    return bool(DSN)


async def init() -> None:
    """Create the pool and ensure the state table exists. Safe to call twice."""
    global _pool, _ready
    if not DSN or _ready:
        return
    try:
        import asyncpg

        _pool = await asyncpg.create_pool(
            DSN,
            ssl="require",
            min_size=1,
            max_size=3,
            command_timeout=20,
        )
        async with _pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS hydra_state (
                    key        text PRIMARY KEY,
                    value      jsonb NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
        _ready = True
        log.info("State store ready (Supabase).")
    except Exception as exc:  # noqa: BLE001 - store must never crash the app
        log.warning("State store disabled: %s", exc)
        _pool = None
        _ready = False


async def get(key: str) -> Optional[Any]:
    if not _ready:
        return None
    try:
        async with _pool.acquire() as conn:
            raw = await conn.fetchval("SELECT value FROM hydra_state WHERE key = $1", key)
        return json.loads(raw) if raw is not None else None
    except Exception as exc:  # noqa: BLE001
        log.warning("store.get(%s) failed: %s", key, exc)
        return None


async def set(key: str, value: Any) -> None:
    """Upsert a value. A value of None deletes the key."""
    if not _ready:
        return
    try:
        async with _pool.acquire() as conn:
            if value is None:
                await conn.execute("DELETE FROM hydra_state WHERE key = $1", key)
            else:
                await conn.execute(
                    """
                    INSERT INTO hydra_state (key, value, updated_at)
                    VALUES ($1, $2::jsonb, now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = EXCLUDED.value, updated_at = now()
                    """,
                    key,
                    json.dumps(value),
                )
    except Exception as exc:  # noqa: BLE001
        log.warning("store.set(%s) failed: %s", key, exc)


def push_soon(key: str, value: Any) -> None:
    """Fire-and-forget write, callable from sync code inside a running loop."""

    async def _task() -> None:
        await set(key, value)

    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(_task())
        _TASKS.add(task)
        task.add_done_callback(_TASKS.discard)
    except RuntimeError:
        pass


async def pull_to_file(key: str, path: Path) -> bool:
    """If `path` is missing but the store has `key`, materialise the file.

    Returns True when the file exists afterwards.
    """
    if path.exists():
        return True
    value = await get(key)
    if value is None:
        return False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(value, (dict, list)):
            path.write_text(json.dumps(value))
        else:
            path.write_text(str(value))
        log.info("Restored %s from state store.", path.name)
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not restore %s: %s", path.name, exc)
        return False
