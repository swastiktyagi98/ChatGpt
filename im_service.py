from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from typing import Dict, Any, List
import json
import asyncio
import time
import uuid
import os
import httpx
import redis.asyncio as redis

app = FastAPI()

redis_client = redis.Redis()

# Connections remain in memory for this process.
connections: Dict[str, WebSocket] = {}

# Default expiration for signal metadata stored in Redis
DEFAULT_SIGNAL_META_TTL = 3600

# Webhook for status updates
WEBHOOK_URL = os.environ.get("STATUS_WEBHOOK_URL")


async def send_status_webhook(user_id: str, status: str) -> None:
    """Post status updates to an external webhook if configured."""
    if not WEBHOOK_URL:
        return
    async with httpx.AsyncClient() as client:
        try:
            await client.post(WEBHOOK_URL, json={"user_id": user_id, "status": status})
        except Exception:
            # Ignore webhook errors to avoid disrupting the websocket flow
            pass


async def deliver_or_queue(user_id: str, payload: Dict[str, Any]):
    """Send payload to a user if connected, otherwise store in Redis."""
    if user_id in connections:
        await connections[user_id].send_json(payload)
    else:
        await redis_client.rpush(f"signals:{user_id}", json.dumps(payload))


async def expire_signal(
    signal_id: str,
    to_user: str,
    from_user: str,
    ttl: int,
    raw_signal: str | None = None,
):
    """Notify sender if a pending signal expires without a response."""
    await asyncio.sleep(ttl)
    status = await redis_client.hget(f"signal:{signal_id}", "status")
    if status is None or status.decode() != "pending":
        return
    if raw_signal:
        await redis_client.lrem(f"signals:{to_user}", 1, raw_signal)
    await redis_client.hset(f"signal:{signal_id}", "status", "expired")
    callback = {
        "action": "signal_expired",
        "to": to_user,
        "id": signal_id,
    }
    await deliver_or_queue(from_user, callback)


class Profile(BaseModel):
    user_id: str = Field(..., description="Unique identifier for the user")
    name: str | None = Field(default=None, description="Display name")
    extra: Dict[str, Any] | None = Field(default=None, description="Arbitrary profile data")



class Status(BaseModel):
    status: str


class Signal(BaseModel):
    from_user: str
    to_user: str
    type: str = "message"
    data: Dict[str, Any] | None = None


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    """Handle WebSocket connections for a given user."""
    await websocket.accept()
    connections[user_id] = websocket

    # Deliver any queued signals stored in Redis upon connection
    while True:
        raw = await redis_client.lpop(f"signals:{user_id}")
        if raw is None:
            break
        msg = json.loads(raw)
        ttl = msg.get("ttl")
        timestamp = msg.get("timestamp", 0)
        if ttl and time.time() - timestamp > ttl:
            # expired before delivery
            callback = {
                "action": "signal_expired",
                "to": user_id,
                "id": msg.get("id"),
                "type": msg.get("type"),
                "data": msg.get("data", {}),
            }
            await deliver_or_queue(msg.get("from"), callback)
            continue
        await websocket.send_json({"action": "signal", **msg})

    try:
        while True:
            message = await websocket.receive_json()
            action = message.get("action")

            if action == "update_profile":
                profile = message.get("profile", {})
                data = json.dumps({
                    "name": profile.get("name"),
                    "extra": profile.get("extra", {})
                })
                await redis_client.set(f"profile:{user_id}", data)
                await websocket.send_json({"result": "profile_updated"})

            elif action == "get_profile":
                target = message.get("user_id", user_id)
                raw = await redis_client.get(f"profile:{target}")
                if raw is None:
                    await websocket.send_json({"error": "profile_not_found"})
                else:
                    profile = json.loads(raw)
                    status = await redis_client.get(f"status:{target}")
                    profile["status"] = status.decode() if status else ""
                    await websocket.send_json({"profile": profile})

            elif action == "set_status":
                status = message.get("status", "")
                await redis_client.set(f"status:{user_id}", status)
                asyncio.create_task(send_status_webhook(user_id, status))
                await websocket.send_json({"result": "status_set"})

            elif action == "signal":
                to_user = message.get("to")
                if not to_user:
                    await websocket.send_json({"error": "missing_to_user"})
                    continue
                ttl = message.get("ttl")
                signal_data = {
                    "id": uuid.uuid4().hex,
                    "from": user_id,
                    "type": message.get("type", "message"),
                    "data": message.get("data", {})
                }
                if ttl:
                    signal_data["ttl"] = ttl
                    signal_data["timestamp"] = time.time()
                # Store minimal metadata for status tracking
                await redis_client.hset(
                    f"signal:{signal_data['id']}",
                    mapping={"from": user_id, "to": to_user, "status": "pending"},
                )
                expire_secs = ttl * 2 if ttl else DEFAULT_SIGNAL_META_TTL
                await redis_client.expire(f"signal:{signal_data['id']}", expire_secs)
                if to_user in connections:
                    await connections[to_user].send_json({"action": "signal", **signal_data})
                else:
                    raw = json.dumps(signal_data)
                    await redis_client.rpush(f"signals:{to_user}", raw)
                    if ttl:
                        asyncio.create_task(
                            expire_signal(signal_data["id"], to_user, user_id, ttl, raw)
                        )
                if ttl and to_user in connections:
                    asyncio.create_task(
                        expire_signal(signal_data["id"], to_user, user_id, ttl)
                    )
                await websocket.send_json({"result": "sent", "id": signal_data["id"]})

            elif action == "respond_signal":
                sig_id = message.get("id")
                response = message.get("response")
                if response not in {"accepted", "rejected"}:
                    await websocket.send_json({"error": "invalid_response"})
                    continue
                key = f"signal:{sig_id}"
                data = await redis_client.hgetall(key)
                if not data or data.get(b"to", b"").decode() != user_id:
                    await websocket.send_json({"error": "signal_not_found"})
                    continue
                status = data.get(b"status", b"").decode()
                if status != "pending":
                    await websocket.send_json({"error": "signal_not_pending"})
                    continue
                await redis_client.hset(key, "status", response)
                from_user = data[b"from"].decode()
                callback = {
                    "action": f"signal_{response}",
                    "from": user_id,
                    "id": sig_id,
                }
                await deliver_or_queue(from_user, callback)
                await websocket.send_json({"result": f"signal_{response}"})

            elif action == "fetch_signals":
                msgs: List[Dict[str, Any]] = []
                while True:
                    raw = await redis_client.lpop(f"signals:{user_id}")
                    if raw is None:
                        break
                    msg = json.loads(raw)
                    ttl = msg.get("ttl")
                    timestamp = msg.get("timestamp", 0)
                    if ttl and time.time() - timestamp > ttl:
                        callback = {
                            "action": "signal_expired",
                            "to": user_id,
                            "id": msg.get("id"),
                            "type": msg.get("type"),
                            "data": msg.get("data", {}),
                        }
                        await deliver_or_queue(msg.get("from"), callback)
                        continue
                    msgs.append(msg)
                await websocket.send_json({"signals": msgs})

            else:
                await websocket.send_json({"error": "unknown_action"})

    except WebSocketDisconnect:
        connections.pop(user_id, None)
