from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import redis.asyncio as redis
import asyncio
import json
from typing import Dict, Set
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Presence API with Real-time WebSocket Push")

# ---------------------------------------------------------------------------
# Connection pool (reused for normal HTTP requests)
# ---------------------------------------------------------------------------
pool = redis.ConnectionPool.from_url(
    "redis://localhost:6379/0",
    max_connections=100,
    decode_responses=True,
)
valkey_client = redis.Redis(connection_pool=pool)


# ---------------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------------
class ConnectionManager:
    """Tracks every open WebSocket and fans-out broadcast messages."""

    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info("WS client connected  | total=%d", len(self.active_connections))

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        logger.info("WS client disconnected | total=%d", len(self.active_connections))

    async def broadcast(self, message: dict):
        """Push JSON to every live client; silently drop dead sockets."""
        dead: Set[WebSocket] = set()
        for ws in self.active_connections:
            try:
                await ws.send_json(message)
            except Exception:
                dead.add(ws)
        self.active_connections -= dead


manager = ConnectionManager()


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------
class HeartbeatRequest(BaseModel):
    user_id: str


# ---------------------------------------------------------------------------
# HTTP endpoints
# ---------------------------------------------------------------------------
@app.post("/heartbeat")
async def update_heartbeat(request: HeartbeatRequest):
    """
    Mark a user as online for 30 s.
    Also publishes to 'presence:updates' so every WebSocket client
    gets the ONLINE event immediately — no polling.
    """
    try:
        await valkey_client.setex(f"presence:{request.user_id}", 30, "1")
        await valkey_client.publish(
            "presence:updates",
            json.dumps({"user_id": request.user_id, "status": "online"}),
        )
        return {"status": "success"}
    except Exception as exc:
        logger.exception("Heartbeat error: %s", exc)
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/status/users")
async def get_users_status(ids: str) -> Dict[str, str]:
    user_ids = [uid.strip() for uid in ids.split(",") if uid.strip()]
    if not user_ids:
        return {}
    keys = [f"presence:{uid}" for uid in user_ids]
    try:
        results = await valkey_client.mget(keys)
        return {
            uid: ("online" if result else "offline")
            for uid, result in zip(user_ids, results)
        }
    except Exception as exc:
        logger.exception("Status error: %s", exc)
        raise HTTPException(status_code=500, detail="Database error")


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------
@app.websocket("/ws/status")
async def websocket_status(websocket: WebSocket):
    """
    Connect once — receive PUSH events forever.
    The server pushes {"user_id": "...", "status": "online"|"offline"}
    the instant a user's state changes. Zero polling.
    """
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data.strip() == "ping":
                await websocket.send_text("pong")   # keep NAT/proxy alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ---------------------------------------------------------------------------
# Background pub/sub listener — bridges Valkey events → WebSocket push
# ---------------------------------------------------------------------------
async def listen_to_pubsub():
    """
    Runs forever alongside the app.

    Subscribes to:
      • presence:updates          — published by /heartbeat (user ONLINE)
      • __keyevent@0__:expired    — Valkey fires this when a TTL key expires
                                    (no heartbeat received → user OFFLINE)
    """
    logger.info("Starting Valkey pub/sub listener…")
    pubsub_client = redis.Redis.from_url(
        "redis://localhost:6379/0", decode_responses=True
    )

    # Enable keyspace notifications: K=keyspace, E=keyevent, x=expired
    await pubsub_client.config_set("notify-keyspace-events", "KEx")

    pubsub = pubsub_client.pubsub()
    await pubsub.subscribe("presence:updates", "__keyevent@0__:expired")
    logger.info("Subscribed to presence:updates + keyevent @expired")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        channel: str = message["channel"]
        data: str = message["data"]

        if channel == "presence:updates":
            try:
                payload = json.loads(data)
                logger.info("→ PUSH online  : %s", payload)
                await manager.broadcast(payload)
            except json.JSONDecodeError:
                logger.warning("Bad JSON on presence:updates: %s", data)

        elif channel == "__keyevent@0__:expired":
            # data = expired key name, e.g. "presence:alice"
            if data.startswith("presence:"):
                user_id = data[len("presence:"):]
                payload = {"user_id": user_id, "status": "offline"}
                logger.info("→ PUSH offline : %s", payload)
                await manager.broadcast(payload)


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    asyncio.create_task(listen_to_pubsub())
    logger.info("Presence API ready — WebSocket push enabled")


@app.on_event("shutdown")
async def shutdown():
    await valkey_client.aclose()