from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import redis.asyncio as redis
import aio_pika
import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Set, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Presence API — Phase 1 + 2")

RABBITMQ_URL  = "amqp://guest:guest@localhost:5672/"
ACTIVITY_QUEUE = "user_activity_logs"

# ---------------------------------------------------------------------------
# Valkey (Redis-compatible) connection pool
# ---------------------------------------------------------------------------
pool = redis.ConnectionPool.from_url(
    "redis://localhost:6379/0",
    max_connections=100,
    decode_responses=True,
)
valkey_client = redis.Redis(connection_pool=pool)

# ---------------------------------------------------------------------------
# RabbitMQ — one connection + one channel shared across all requests
# (aio-pika channels are NOT thread-safe but are coroutine-safe)
# ---------------------------------------------------------------------------
rabbitmq_connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
rabbitmq_channel:    Optional[aio_pika.abc.AbstractChannel]           = None


async def get_rabbitmq_channel() -> aio_pika.abc.AbstractChannel:
    """Return the shared channel, reconnecting if it was closed."""
    global rabbitmq_connection, rabbitmq_channel
    if rabbitmq_connection is None or rabbitmq_connection.is_closed:
        rabbitmq_connection = await aio_pika.connect_robust(RABBITMQ_URL)
        rabbitmq_channel    = await rabbitmq_connection.channel()
        # Declare the queue so it exists even if the worker hasn't started yet.
        # durable=True means the queue survives a RabbitMQ restart.
        await rabbitmq_channel.declare_queue(ACTIVITY_QUEUE, durable=True)
        logger.info("RabbitMQ channel ready")
    return rabbitmq_channel


async def publish_activity(user_id: str, event: str) -> None:
    """
    Fire-and-forget: push a JSON message onto the user_activity_logs queue.

    FastAPI never waits for the worker to process it — it returns success
    to the caller immediately after the publish call returns.

    delivery_mode=PERSISTENT tells RabbitMQ to write the message to disk
    so it is not lost if the broker restarts before the worker consumes it.
    """
    message_body = {
        "user_id":   user_id,
        "event":     event,                                    # connected | disconnected | heartbeat
        "timestamp": datetime.now(timezone.utc).isoformat(),  # ISO-8601 UTC
    }
    channel = await get_rabbitmq_channel()
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(message_body).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        ),
        routing_key=ACTIVITY_QUEUE,
    )
    logger.info("→ MQ published  : %s", message_body)


# ---------------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------------
class ConnectionManager:
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
        dead: Set[WebSocket] = set()
        for ws in self.active_connections:
            try:
                await ws.send_json(message)
            except Exception:
                dead.add(ws)
        self.active_connections -= dead


manager = ConnectionManager()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class HeartbeatRequest(BaseModel):
    user_id: str


# ---------------------------------------------------------------------------
# HTTP endpoints
# ---------------------------------------------------------------------------
@app.post("/heartbeat")
async def update_heartbeat(request: HeartbeatRequest):
    """
    1. Set presence key in Valkey (30 s TTL).
    2. Publish ONLINE event to Valkey pub/sub → WebSocket push.
    3. Publish 'heartbeat' activity to RabbitMQ → worker logs it to Postgres.
    Returns immediately — the DB write happens asynchronously in the worker.
    """
    try:
        # --- Valkey: presence + real-time push (Phase 1) ---
        await valkey_client.setex(f"presence:{request.user_id}", 30, "1")
        await valkey_client.publish(
            "presence:updates",
            json.dumps({"user_id": request.user_id, "status": "online"}),
        )

        # --- RabbitMQ: async activity log (Phase 2) ---
        await publish_activity(request.user_id, "heartbeat")

        return {"status": "success"}
    except Exception as exc:
        logger.exception("Heartbeat error: %s", exc)
        raise HTTPException(status_code=500, detail="Internal error")


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
        raise HTTPException(status_code=500, detail="Internal error")


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------
@app.websocket("/ws/status")
async def websocket_status(websocket: WebSocket):
    """
    Phase 1  — push Valkey presence events to this client.
    Phase 2  — log connect / disconnect events to RabbitMQ for the worker.
    """
    # Extract an optional user_id from query params: ws://host/ws/status?user_id=alice
    user_id = websocket.query_params.get("user_id", "anonymous")

    await manager.connect(websocket)

    # Publish "connected" activity — FastAPI does NOT write to Postgres directly
    await publish_activity(user_id, "connected")

    try:
        while True:
            data = await websocket.receive_text()
            if data.strip() == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        # Publish "disconnected" activity
        await publish_activity(user_id, "disconnected")


# ---------------------------------------------------------------------------
# Background Valkey pub/sub listener (Phase 1 — unchanged)
# ---------------------------------------------------------------------------
async def listen_to_pubsub():
    logger.info("Starting Valkey pub/sub listener…")
    pubsub_client = redis.Redis.from_url(
        "redis://localhost:6379/0", decode_responses=True
    )
    await pubsub_client.config_set("notify-keyspace-events", "KEx")
    pubsub = pubsub_client.pubsub()
    await pubsub.subscribe("presence:updates", "__keyevent@0__:expired")
    logger.info("Subscribed to presence:updates + keyevent @expired")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        channel: str = message["channel"]
        data: str    = message["data"]

        if channel == "presence:updates":
            try:
                payload = json.loads(data)
                logger.info("→ PUSH online  : %s", payload)
                await manager.broadcast(payload)
            except json.JSONDecodeError:
                logger.warning("Bad JSON on presence:updates: %s", data)

        elif channel == "__keyevent@0__:expired":
            if data.startswith("presence:"):
                uid = data[len("presence:"):]
                payload = {"user_id": uid, "status": "offline"}
                logger.info("→ PUSH offline : %s", payload)
                await manager.broadcast(payload)


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    # Warm up RabbitMQ connection so the first request is not slow
    await get_rabbitmq_channel()
    asyncio.create_task(listen_to_pubsub())
    logger.info("Presence API ready — Phase 1 (WebSocket push) + Phase 2 (MQ) active")


@app.on_event("shutdown")
async def shutdown():
    await valkey_client.aclose()
    if rabbitmq_connection and not rabbitmq_connection.is_closed:
        await rabbitmq_connection.close()
        logger.info("RabbitMQ connection closed")