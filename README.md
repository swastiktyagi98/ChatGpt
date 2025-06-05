# Simple IM Service

This example demonstrates a lightweight Instant Messaging (IM) backend written with **FastAPI**. It provides a few basic features similar to an IM SDK so that mobile and web applications can experiment with signalling, user profiles and custom statuses without relying on a third‑party provider.

## Features

- **User profiles** &ndash; create and update profile information.
- **Custom status** &ndash; set a text status for a user (e.g. "Busy", "Available").
- **Signalling** &ndash; send messages/signals between users that can be fetched later.

Data is stored in a Redis instance so multiple workers can share state.

## Setup

Install the dependencies:

```bash
pip install -r requirements.txt
```

Ensure a Redis server is running locally (default localhost:6379). On Ubuntu you can install it with:

```bash
sudo apt-get install redis-server
```

### Webhook for status updates

Set the `STATUS_WEBHOOK_URL` environment variable to the URL of your Laravel
backend to receive notifications whenever a user updates their status. The
service will POST a JSON object containing `user_id` and `status`.

## Running

Start the service using Uvicorn:

```bash
uvicorn im_service:app --reload
```

## WebSocket API

All interaction happens over a WebSocket connection. Connect to the
`/ws/{user_id}` endpoint and send JSON messages containing an `action` field.

Example using [websocat](https://github.com/vi/websocat):

```bash
websocat ws://localhost:8000/ws/alice
```

### Actions

- `update_profile` – supply a `profile` object to create or update your profile
- `get_profile` – return your profile or specify `user_id` to fetch another
  user's profile
- `set_status` – set a custom status string
- `signal` – send a signalling message to another user
- `respond_signal` – accept or reject a received signal
- `fetch_signals` – retrieve any queued signals

Signals can include an optional `ttl` (in seconds) controlling how long the
message waits for a response. If the recipient does not accept or reject the
signal within this period, the sender receives a `signal_expired` callback.
When the recipient responds using `respond_signal`, the sender will get either
`signal_accepted` or `signal_rejected` as appropriate.

Example sequence:

```json
{"action": "update_profile", "profile": {"name": "Alice"}}
{"action": "set_status", "status": "Busy"}
{"action": "signal", "to": "bob", "data": {"text": "hi"}, "ttl": 30}
{"action": "respond_signal", "id": "<signal_id>", "response": "accepted"}
```
The last message shows Bob accepting the signal using the identifier returned
when Alice sent it.

## Scaling to Many Users

Redis allows running multiple `uvicorn` workers or even multiple servers behind
a load balancer. Start Uvicorn with several workers to handle thousands of
concurrent WebSocket connections:

```bash
uvicorn im_service:app --workers 4
```

Adjust the worker count based on your server's CPU cores.
