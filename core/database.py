"""
core/database.py
Async MongoDB interface via Motor.
Handles accounts, join source configs, and per-account joined link tracking.
"""

import logging
from typing import Optional
from datetime import datetime, timezone

import motor.motor_asyncio
from pymongo import ReturnDocument

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, uri: str, db_name: str):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self._db = self._client[db_name]

        self._accounts     = self._db["accounts"]
        self._join_configs = self._db["join_configs"]   # source channels per account
        self._joined_links = self._db["joined_links"]   # links already joined per account

    # ──────────────────────────────────────────────
    # Setup / Indexes
    # ──────────────────────────────────────────────

    async def setup(self):
        """Create all required indexes. Safe to call on every startup."""
        await self._accounts.create_index("user_id", unique=True)
        await self._join_configs.create_index("user_id", unique=True)
        # compound index: one link per account (no duplicates)
        await self._joined_links.create_index(
            [("user_id", 1), ("link", 1)], unique=True
        )
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
        doc = {
            "user_id":        user_id,
            "name":           name,
            "username":       username,
            "session_string": session_string,
        }
        result = await self._accounts.find_one_and_update(
            {"user_id": user_id},
            {"$set": doc},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        logger.info("Account %s (%s) saved.", name, user_id)
        return result.get("_id") is not None

    async def remove_account(self, user_id: int) -> bool:
        """
        Remove account document AND all associated join data.
        Single call cleans everything.
        """
        result = await self._accounts.delete_one({"user_id": user_id})
        deleted = result.deleted_count > 0

        if deleted:
            # cascade delete join data
            await self._join_configs.delete_one({"user_id": user_id})
            lnk_result = await self._joined_links.delete_many({"user_id": user_id})
            logger.info(
                "Account %s removed. Cleaned %d joined link records.",
                user_id, lnk_result.deleted_count
            )
        else:
            logger.warning("Tried to remove non-existent account %s.", user_id)

        return deleted

    async def get_account(self, user_id: int) -> Optional[dict]:
        return await self._accounts.find_one({"user_id": user_id}, {"_id": 0})

    async def get_all_accounts(self) -> list[dict]:
        cursor = self._accounts.find({}, {"_id": 0})
        return await cursor.to_list(length=None)

    # ──────────────────────────────────────────────
    # Join Source Channel Config  (per account)
    # ──────────────────────────────────────────────

    async def add_source_channel(self, user_id: int, channel_id: int) -> bool:
        """Add a source channel to an account's join config. Returns True if added (not duplicate)."""
        result = await self._join_configs.update_one(
            {"user_id": user_id},
            {"$addToSet": {"source_channels": channel_id}},
            upsert=True,
        )
        added = result.modified_count > 0 or result.upserted_id is not None
        if added:
            logger.info("Source channel %s added for account %s.", channel_id, user_id)
        return added

    async def remove_source_channel(self, user_id: int, channel_id: int) -> bool:
        """Remove a source channel from an account's config."""
        result = await self._join_configs.update_one(
            {"user_id": user_id},
            {"$pull": {"source_channels": channel_id}},
        )
        return result.modified_count > 0

    async def get_source_channels(self, user_id: int) -> list[int]:
        """Return list of source channel IDs for this account."""
        doc = await self._join_configs.find_one({"user_id": user_id}, {"_id": 0})
        if not doc:
            return []
        return doc.get("source_channels", [])

    # ──────────────────────────────────────────────
    # Joined Links Tracking  (per account)
    # ──────────────────────────────────────────────

    async def is_link_joined(self, user_id: int, link: str) -> bool:
        """Check if this account has already joined this link."""
        doc = await self._joined_links.find_one(
            {"user_id": user_id, "link": link}
        )
        return doc is not None

    async def mark_link_joined(self, user_id: int, link: str):
        """Record that this account has joined this link."""
        try:
            await self._joined_links.insert_one({
                "user_id":   user_id,
                "link":      link,
                "joined_at": datetime.now(timezone.utc),
            })
        except Exception:
            pass  # duplicate key — already marked, safe to ignore

    async def get_joined_count(self, user_id: int) -> int:
        """Total joined links for an account."""
        return await self._joined_links.count_documents({"user_id": user_id})

    async def clear_joined_links(self, user_id: int) -> int:
        """
        Manually clear join history for an account
        (so it can re-join everything). Returns count deleted.
        """
        result = await self._joined_links.delete_many({"user_id": user_id})
        return result.deleted_count

    # ──────────────────────────────────────────────
    # Teardown
    # ──────────────────────────────────────────────

    def close(self):
        self._client.close()
        logger.info("Database connection closed.")
