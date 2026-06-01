"""
core/worker_map.py
In-memory registry that maps Telegram user_id → live TelegramClient instances.
Allows hot-add / hot-remove without restarting the bot.
"""

import logging
import asyncio
from typing import Optional
from telethon import TelegramClient
from telethon.sessions import StringSession

logger = logging.getLogger(__name__)


class WorkerMap:
    def __init__(self, api_id: int, api_hash: str):
        self._api_id = api_id
        self._api_hash = api_hash
        # { user_id (int) → TelegramClient }
        self._workers: dict[int, TelegramClient] = {}

    # ──────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────

    async def add_worker(self, user_id: int, session_string: str) -> TelegramClient:
        """
        Instantiate and connect a TelegramClient from a StringSession.
        If a client for this user_id already exists, it is replaced.
        """
        # Disconnect stale client if present
        await self.remove_worker(user_id)

        client = TelegramClient(
            StringSession(session_string),
            self._api_id,
            self._api_hash,
        )
        await client.connect()

        if not await client.is_user_authorized():
            await client.disconnect()
            raise PermissionError(
                f"Session for user_id={user_id} is not authorised."
            )

        self._workers[user_id] = client
        logger.info("Worker started for user_id=%s", user_id)
        return client

    async def remove_worker(self, user_id: int) -> bool:
        """Disconnect and remove a worker. Returns True if it existed."""
        client = self._workers.pop(user_id, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass  # Best-effort disconnect
            logger.info("Worker stopped for user_id=%s", user_id)
            return True
        return False

    async def stop_all(self):
        """Disconnect every worker on shutdown."""
        tasks = [self.remove_worker(uid) for uid in list(self._workers)]
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("All workers stopped.")

    # ──────────────────────────────────────────────
    # Accessors
    # ──────────────────────────────────────────────

    def get(self, user_id: int) -> Optional[TelegramClient]:
        return self._workers.get(user_id)

    def all_ids(self) -> list[int]:
        return list(self._workers.keys())

    def count(self) -> int:
        return len(self._workers)
