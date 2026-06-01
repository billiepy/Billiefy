"""
main.py
Orchestrator: connects to MongoDB, boots worker clients for all
stored accounts, then runs the master bot until interrupted.
"""

import asyncio
import logging
import os
from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.sessions import StringSession

from core.database import Database
from core.worker_map import WorkerMap
from plugins.account_manager import register as register_account_manager

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("main")


async def main():
    # ── Config ──────────────────────────────────────────────────────────────
    BOT_TOKEN  = os.environ["BOT_TOKEN"]
    API_ID     = int(os.environ["API_ID"])
    API_HASH   = os.environ["API_HASH"]
    MONGO_URI  = os.environ["MONGO_URI"]
    MONGO_DB   = os.environ.get("MONGO_DB_NAME", "tg_admin_dashboard")

    # ── Database ─────────────────────────────────────────────────────────────
    db = Database(MONGO_URI, MONGO_DB)
    await db.setup()

    # ── Worker map ───────────────────────────────────────────────────────────
    workers = WorkerMap(API_ID, API_HASH)

    # Boot workers for every persisted account
    all_accounts = await db.get_all_accounts()
    logger.info("Booting %d stored worker(s)…", len(all_accounts))
    for acc in all_accounts:
        try:
            await workers.add_worker(acc["user_id"], acc["session_string"])
            logger.info("  ✓  %s (%s)", acc["name"], acc["user_id"])
        except Exception as exc:
            logger.warning("  ✗  %s — %s", acc["name"], exc)

    # ── Master bot ────────────────────────────────────────────────────────────
    bot = TelegramClient("bot_session", API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("Master bot started.")

    # ── Plugin registration ───────────────────────────────────────────────────
    register_account_manager(bot, db, workers)

    # ── Run until Ctrl-C ──────────────────────────────────────────────────────
    try:
        await bot.run_until_disconnected()
    finally:
        await workers.stop_all()
        db.close()
        logger.info("Clean shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
