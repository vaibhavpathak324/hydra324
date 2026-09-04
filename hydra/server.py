from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from hydra import store
from hydra.bot import controller
from hydra.engine import DATA, engine

log = logging.getLogger("hydra.server")
WEB = Path(__file__).resolve().parent.parent / "web"


@asynccontextmanager
async def lifespan(app: FastAPI):
    DATA.mkdir(parents=True, exist_ok=True)
    await store.init()
    await engine.try_resume()
    try:
        await controller.start()
    except Exception as exc:
        log.warning("Control bot did not start: %s", exc)
        engine.note("warn", f"Control bot did not start: {exc}")
    yield
    try:
        await controller.stop()
    except Exception:
        pass
    if engine.client:
        try:
            await engine.client.disconnect()
        except Exception:
            pass


app = FastAPI(title="HYDRA 324", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class LoginStart(BaseModel):
    api_id: int
    api_hash: str
    phone: str


class LoginCode(BaseModel):
    code: str


class LoginPassword(BaseModel):
    password: str


class LoginString(BaseModel):
    api_id: int
    api_hash: str
    session_string: str


class PeoplePayload(BaseModel):
    chat_id: int
    message: str = Field(min_length=1)
    people: list[dict[str, Any]]


class BotToken(BaseModel):
    token: str


class ScanBody(BaseModel):
    chats: list[dict[str, Any]]


def _fail(exc: Exception) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc) or exc.__class__.__name__)


@app.get("/")
async def index():
    return FileResponse(WEB / "index.html")


@app.get("/api/status")
async def status():
    snap = engine.snapshot()
    snap["bot"] = controller.status()
    snap["defaults"] = {
        "api_id": (os.environ.get("TELEGRAM_API_ID") or os.environ.get("API_ID") or "").strip(),
        "api_hash": (os.environ.get("TELEGRAM_API_HASH") or os.environ.get("API_HASH") or "").strip(),
    }
    return snap


@app.post("/api/auth/start")
async def auth_start(body: LoginStart):
    try:
        return await engine.start_login(body.api_id, body.api_hash, body.phone)
    except Exception as exc:
        raise _fail(exc)


@app.post("/api/auth/code")
async def auth_code(body: LoginCode):
    try:
        return await engine.submit_code(body.code)
    except Exception as exc:
        raise _fail(exc)


@app.post("/api/auth/password")
async def auth_password(body: LoginPassword):
    try:
        return await engine.submit_password(body.password)
    except Exception as exc:
        raise _fail(exc)


@app.post("/api/auth/string")
async def auth_string(body: LoginString):
    try:
        return await engine.login_string(body.api_id, body.api_hash, body.session_string)
    except Exception as exc:
        raise _fail(exc)


@app.post("/api/auth/logout")
async def auth_logout():
    await engine.logout()
    return engine.snapshot()


@app.get("/api/session-string")
async def session_string():
    raw = engine.export_session_string()
    if not raw:
        raise HTTPException(status_code=400, detail="No live session.")
    return {"session_string": raw}


@app.get("/api/bot")
async def bot_status():
    return controller.status()


@app.post("/api/bot/start")
async def bot_start(body: BotToken):
    try:
        return await controller.start(body.token.strip())
    except Exception as exc:
        raise _fail(exc)


@app.post("/api/bot/stop")
async def bot_stop():
    await controller.stop()
    return controller.status()


@app.get("/api/chats")
async def chats():
    try:
        return {"chats": await engine.list_chats()}
    except Exception as exc:
        raise _fail(exc)


@app.post("/api/chats/scan")
async def scan(body: ScanBody):
    try:
        return {"chats": await engine.scan_pending(body.chats)}
    except Exception as exc:
        raise _fail(exc)


@app.get("/api/requests")
async def requests(chat_id: int):
    try:
        return {"people": await engine.list_requests(chat_id)}
    except Exception as exc:
        raise _fail(exc)


@app.post("/api/arm")
async def arm(body: PeoplePayload):
    try:
        return await engine.arm_drafts(body.chat_id, body.people, body.message)
    except Exception as exc:
        raise _fail(exc)


@app.post("/api/fire")
async def fire():
    try:
        return await engine.fire_drafts()
    except Exception as exc:
        raise _fail(exc)


@app.post("/api/send")
async def send(body: PeoplePayload):
    try:
        return await engine.send_now(body.chat_id, body.people, body.message)
    except Exception as exc:
        raise _fail(exc)


@app.post("/api/disarm")
async def disarm():
    try:
        return await engine.disarm()
    except Exception as ext:
        raise _fail(ext)


@app.get("/api/armed")
async def armed():
    return {"armed": engine.armed_list(), "count": len(engine.armed)}


@app.get("/api/logs")
async def logs():
    return {"logs": list(engine.logs)}


@app.websocket("/ws")
async def ws(socket: WebSocket):
    await socket.accept()
    q = engine.listen()
    try:
        await socket.send_json({"event": "status", **engine.snapshot(), "bot": controller.status()})
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=20)
                await socket.send_json(msg)
            except asyncio.TimeoutError:
                await socket.send_json({"event": "ping"})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        engine.unlisten(q)


app.mount("/static", StaticFiles(directory=str(WEB)), name="static")
