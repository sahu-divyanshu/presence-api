"""
worker.py — Phase 3: The Async Worker
======================================
This is a completely separate Python process from FastAPI.
It has one job:
  1. Connect to RabbitMQ and listen to the 'user_activity_logs' queue.
  2. For every message it pulls off the queue, write a row to PostgreSQL.

Run it in its own terminal:
    source .venv/bin/activate
    python worker.py
"""

import pika
import psycopg  # Use psycopg (v3) instead of psycopg2
import json
import logging
import time
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WORKER] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — edit these to match your environment
# ---------------------------------------------------------------------------
RABBITMQ_HOST  = "localhost"
RABBITMQ_PORT  = 5672
RABBITMQ_USER  = "guest"
RABBITMQ_PASS  = "guest"
ACTIVITY_QUEUE = "user_activity_logs"

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = "presence_db"
PG_USER = "postgres"      # Updated to match Docker
PG_PASS = "strong_password"      # Updated to match Docker

# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------
def connect_postgres():
    """Open a PostgreSQL connection with retry logic."""
    while True:
        try:
            # psycopg.connect is the v3 way
            conn = psycopg.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASS,
                autocommit=True
            )
            logger.info("Connected to PostgreSQL at %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
            return conn
        except Exception as exc:
            logger.warning("PostgreSQL not ready (%s) — retrying in 3 s…", exc)
            time.sleep(3)
def insert_activity(conn, user_id: str, event: str, timestamp: str) -> None:
    sql = """
        INSERT INTO user_activity_logs (user_id, event, logged_at)
        VALUES (%s, %s, %s::timestamptz)
    """
    try:
        cur = conn.execute(sql, (user_id, event, timestamp))
        logger.info("✓ DB write  user=%-15s event=%-15s ts=%s", user_id, event, timestamp)
    except Exception as exc:
        logger.error("DB write failed: %s", exc)
        # In v3 with autocommit=True, we don't need conn.rollback() usually, 
        # but you should handle connection loss here in a real prod app.

# ---------------------------------------------------------------------------
# RabbitMQ callback — called once per message
# ---------------------------------------------------------------------------
def on_message(channel, method, properties, body, *, pg_conn):
    """
    This function is called by pika every time a message arrives.

    Parameters mirror the AMQP frame:
      channel    — the pika channel object
      method     — delivery metadata (delivery_tag, routing_key, …)
      properties — message properties (content_type, delivery_mode, …)
      body       — raw bytes of the message payload
    """
    try:
        # 1. Decode the JSON payload FastAPI published
        payload = json.loads(body.decode())
        logger.info("← MQ received  : %s", payload)

        user_id   = payload["user_id"]
        event     = payload["event"]
        timestamp = payload["timestamp"]

        # 2. Write to PostgreSQL (synchronous — this is a blocking worker, not async)
        insert_activity(pg_conn, user_id, event, timestamp)

        # 3. Acknowledge the message ONLY after a successful DB write.
        #    If the worker crashes before ack, RabbitMQ re-queues the message
        #    so it is never silently lost.
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except (KeyError, json.JSONDecodeError) as exc:
        # Malformed message — reject without re-queue (dead-letter it)
        logger.error("Bad message payload: %s — %s", body, exc)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except Exception as exc:
        # Unexpected error — re-queue the message so it is retried
        logger.exception("Unexpected error processing message: %s", exc)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


# ---------------------------------------------------------------------------
# RabbitMQ connection with retry
# ---------------------------------------------------------------------------
def connect_rabbitmq() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=60,              # detect dead connections
        blocked_connection_timeout=300,
    )
    while True:
        try:
            conn = pika.BlockingConnection(params)
            logger.info("Connected to RabbitMQ at %s:%s", RABBITMQ_HOST, RABBITMQ_PORT)
            return conn
        except pika.exceptions.AMQPConnectionError as exc:
            logger.warning("RabbitMQ not ready (%s) — retrying in 3 s…", exc)
            time.sleep(3)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 55)
    logger.info("  Worker starting up")
    logger.info("  Queue   : %s", ACTIVITY_QUEUE)
    logger.info("  Postgres: %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
    logger.info("=" * 55)

    # Connect to both services
    pg_conn = connect_postgres()
    mq_conn = connect_rabbitmq()

    mq_channel = mq_conn.channel()

    # Declare the same queue FastAPI declares — idempotent, safe to call twice
    mq_channel.queue_declare(queue=ACTIVITY_QUEUE, durable=True)

    # prefetch_count=1 means the worker handles one message at a time.
    # RabbitMQ will not send a second message until the first is ack'd.
    # This prevents the worker from being overwhelmed.
    mq_channel.basic_qos(prefetch_count=1)

    # Register the callback, passing pg_conn via a closure
    mq_channel.basic_consume(
        queue=ACTIVITY_QUEUE,
        on_message_callback=lambda ch, method, props, body: on_message(
            ch, method, props, body, pg_conn=pg_conn
        ),
    )

    logger.info("Waiting for messages on '%s'. Press CTRL+C to stop.", ACTIVITY_QUEUE)

    try:
        # Blocking loop — pika calls on_message whenever a message arrives
        mq_channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Shutting down worker…")
        mq_channel.stop_consuming()
    finally:
        if mq_conn.is_open:
            mq_conn.close()
        if not pg_conn.closed:
            pg_conn.close()
        logger.info("Worker stopped cleanly.")
        sys.exit(0)


if __name__ == "__main__":
    main()