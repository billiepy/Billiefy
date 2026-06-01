"""
core/database.py
Async MongoDB interface via Motor.
Handles all CRUD operations for stored Telegram accounts.
"""

import logging
from typing import Optional
import motor.motor_asyncio
from pymongo import ReturnDocument

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, uri: str, db_name: str):
        """
        Initialize the Motor async client.
        
        :param uri:     MongoDB connection URI
        :param db_name: Target database name
        """
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self._db = self._client[db_name]
        self._accounts = self._db["accounts"]

    # ──────────────────────────────────────────────
    # Index / setup
    # ──────────────────────────────────────────────

    async def setup(self):
        """Create indexes on first run. Safe to call on every startup."""
        await self._accounts.create_index("user_id", unique=True)
        logger.info("Database indexes ensured.")

    # ──────────────────────────────────────────────
    # Account CRUD
    # ──────────────────────────────────────────────

    async def add_account(
        self,
        user_id: int,
        name: str,
        username: Optional[str],
        session_string: str,
    ) -> bool:
        """
        Insert or update an account document.

        :param user_id:        Telegram user ID of the managed account
        :param name:           Display name (first + last)
        :param username:       @username (may be None)
        :param session_string: Serialised Telethon StringSession
        :return:               True if inserted, False if already existed (upserted)
        """
        doc = {
            "user_id": user_id,
            "name": name,
            "username": username,
            "session_string": session_string,
        }
        result = await self._accounts.find_one_and_update(
            {"user_id": user_id},
            {"$set": doc},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        inserted = result.get("_id") is not None
        logger.info("Account %s (%s) saved to DB.", name, user_id)
        return inserted

    async def remove_account(self, user_id: int) -> bool:
        """
        Delete an account document by Telegram user ID.

        :return: True if a document was deleted, False if not found.
        """
        result = await self._accounts.delete_one({"user_id": user_id})
        deleted = result.deleted_count > 0
        if deleted:
            logger.info("Account %s removed from DB.", user_id)
        else:
            logger.warning("Tried to remove non-existent account %s.", user_id)
        return deleted

    async def get_account(self, user_id: int) -> Optional[dict]:
        """Fetch a single account document by Telegram user ID."""
        return await self._accounts.find_one(
            {"user_id": user_id}, {"_id": 0}
        )

    async def get_all_accounts(self) -> list[dict]:
        """Return all stored account documents (without Mongo _id)."""
        cursor = self._accounts.find({}, {"_id": 0})
        return await cursor.to_list(length=None)

    # ──────────────────────────────────────────────
    # Teardown
    # ──────────────────────────────────────────────

    def close(self):
        """Close the Motor client gracefully."""
        self._client.close()
        logger.info("Database connection closed.")
