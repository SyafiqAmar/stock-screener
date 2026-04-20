"""
WebSocket handler for real-time signal updates.
"""
import json
import asyncio
import logging
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages active WebSocket connections and broadcasts."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self._queue: asyncio.Queue = asyncio.Queue()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Send message to all connected clients."""
        dead = []
        for conn in self.active_connections:
            try:
                await conn.send_json(message)
            except Exception:
                dead.append(conn)
        for d in dead:
            self.disconnect(d)

    async def send_personal(self, websocket: WebSocket, message: dict):
        """Send message to a specific client."""
        try:
            await websocket.send_json(message)
        except Exception:
            self.disconnect(websocket)

    def push_event(self, event: dict):
        """Push an event to the broadcast queue (called from sync code)."""
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            pass  # Drop if queue is full


# Global manager instance
manager = ConnectionManager()


async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for live updates.

    Events pushed to clients:
        - scan_started: { type, category, timeframes }
        - scan_progress: { type, completed, total, current_ticker }
        - scan_complete: { type, total_signals }
        - new_signal: { type, ticker, signal_type, confidence }
    """
    await manager.connect(websocket)

    try:
        # Send welcome message
        await manager.send_personal(websocket, {
            "type": "connected",
            "message": "Connected to Stock Screener live updates",
            "clients": len(manager.active_connections),
        })

        while True:
            try:
                # Wait for client messages (heartbeat/subscribe)
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                msg = json.loads(data)

                if msg.get("type") == "ping":
                    await manager.send_personal(websocket, {"type": "pong"})
                elif msg.get("type") == "subscribe":
                    await manager.send_personal(websocket, {
                        "type": "subscribed",
                        "channel": msg.get("channel", "all"),
                    })

            except asyncio.TimeoutError:
                # Send heartbeat
                await manager.send_personal(websocket, {"type": "heartbeat"})

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)
