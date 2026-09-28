```python
"""Bare WebSocket relay. Connect two clients to the same room, whatever one
sends gets forwarded straight to the other. No auth, no framing, no parsing —
just raw pass-through, as fast as possible.

    GET  /health              -> 200 ok
    WS   /ws?room=NAME         -> joins room NAME (default: "default")

Only dependency: aiohttp.
    pip install -r requirements.txt
    python relay.py
"""

import os
from aiohttp import web, WSMsgType

rooms = {}  # room name -> set of connected websockets


async def health(request):
    return web.Response(text="ok")


async def ws_handler(request):
    room = request.query.get("room", "default")
    ws = web.WebSocketResponse(max_msg_size=0, heartbeat=20)
    await ws.prepare(request)

    peers = rooms.setdefault(room, set())
    peers.add(ws)

    try:
        async for msg in ws:
            if msg.type not in (WSMsgType.TEXT, WSMsgType.BINARY):
                if msg.type == WSMsgType.ERROR:
                    break
                continue

            # Snapshot the set so peer cleanup cannot interfere with
            # iteration while messages are being forwarded.
            for peer in tuple(peers):
                if peer is ws or peer.closed:
                    continue

                try:
                    if msg.type == WSMsgType.TEXT:
                        await peer.send_str(msg.data)
                    else:
                        await peer.send_bytes(msg.data)
                except (ConnectionError, RuntimeError):
                    # The peer may have disconnected between the closed
                    # check and the send.
                    peers.discard(peer)

    finally:
        peers.discard(ws)

        if not peers:
            rooms.pop(room, None)

    return ws


app = web.Application()
app.router.add_get("/health", health)
app.router.add_get("/ws", ws_handler)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    web.run_app(app, host="0.0.0.0", port=port)
```

### What changed from the original

**1. Stable peer snapshot**

```python
for peer in tuple(peers):
```

instead of:

```python
for peer in peers:
```

This makes the forwarding loop safer when connections are being removed concurrently.

**2. Failed sends no longer break the whole relay**

```python
try:
    ...
except (ConnectionError, RuntimeError):
    peers.discard(peer)
```

A disconnected peer is simply removed.

**3. Message-type handling is slightly cleaner**

Instead of nesting everything under the positive condition, unsupported WebSocket events are handled explicitly.

**4. Original architecture is preserved**

No authentication, database, Redis, message parsing, classes, framework change, or additional dependencies.

That's important for a contribution because you're improving the implementation **without changing what the project is supposed to be**.

One thing I'd consider **before submitting the PR**: add a small test suite alongside this change. A PR titled something like **"Handle disconnected peers safely during relay"** would be much easier for a maintainer to review than a broad "improve relay" PR.
