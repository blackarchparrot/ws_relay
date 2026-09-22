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
            if msg.type in (WSMsgType.TEXT, WSMsgType.BINARY):
                for peer in peers:
                    if peer is not ws and not peer.closed:
                        if msg.type == WSMsgType.TEXT:
                            await peer.send_str(msg.data)
                        else:
                            await peer.send_bytes(msg.data)
            elif msg.type == WSMsgType.ERROR:
                break
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
