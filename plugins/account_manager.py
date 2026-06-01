"""
plugins/account_manager.py
Handles:
  - /start  → main menu
  - Add Account flow  (session string input → validate → persist)
  - Remove Account flow (select → confirm → delete)
  - Account sub-menu  (per-account actions)

Conversation state is tracked in a lightweight in-memory dict keyed
by (chat_id, user_id) so multiple admins can use the bot concurrently.
"""

import logging
import asyncio
from typing import Any

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import AuthKeyError, SessionPasswordNeededError

from core.database import Database
from core.worker_map import WorkerMap

logger = logging.getLogger(__name__)

# ── Conversation state keys ──────────────────────────────────────────────────
# Stored as  _conv_state[(chat_id, sender_id)] = { "step": ..., "data": ... }
_conv_state: dict[tuple, dict] = {}

STEP_IDLE          = "idle"
STEP_AWAIT_SESSION = "awaiting_session_string"


# ─────────────────────────────────────────────────────────────────────────────
# Helper: build keyboards
# ─────────────────────────────────────────────────────────────────────────────

def _main_menu_keyboard() -> list:
    return [
        [Button.inline("📋  View Accounts",  b"menu:view")],
        [Button.inline("➕  Add Account",    b"menu:add")],
        [Button.inline("➖  Remove Account", b"menu:remove")],
    ]


def _accounts_keyboard(accounts: list[dict]) -> list:
    """One button per account + a Back button."""
    rows = [
        [Button.inline(
            f"👤  {a['name']}  (@{a['username'] or 'no_username'})",
            f"acc:{a['user_id']}".encode(),
        )]
        for a in accounts
    ]
    rows.append([Button.inline("⬅️  Back", b"menu:back")])
    return rows


def _remove_list_keyboard(accounts: list[dict]) -> list:
    rows = [
        [Button.inline(
            f"🗑  {a['name']}",
            f"rm_select:{a['user_id']}".encode(),
        )]
        for a in accounts
    ]
    rows.append([Button.inline("⬅️  Back", b"menu:back")])
    return rows


def _confirm_remove_keyboard(user_id: int) -> list:
    return [
        [
            Button.inline("✅  Yes, remove", f"rm_confirm:{user_id}".encode()),
            Button.inline("❌  Cancel",       b"menu:back"),
        ]
    ]


def _account_action_keyboard(user_id: int) -> list:
    uid = str(user_id)
    return [
        [Button.inline("📢  Broadcast GC",       f"act:bc_gc:{uid}".encode())],
        [Button.inline("📨  Broadcast DM",        f"act:bc_dm:{uid}".encode())],
        [Button.inline("🔇  Leave Muted Groups",  f"act:leave_muted:{uid}".encode())],
        [Button.inline("🔗  Start Joining Groups", f"act:join_groups:{uid}".encode())],
        [Button.inline("🖼  Profile Management",  f"act:profile:{uid}".encode())],
        [Button.inline("⬅️  Back",                b"menu:back")],
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Registration entry-point
# ─────────────────────────────────────────────────────────────────────────────

def register(bot: TelegramClient, db: Database, workers: WorkerMap):
    """
    Attach all event handlers to the master bot client.
    Called once from main.py after the bot is connected.
    """

    # ── /start ────────────────────────────────────────────────────────────────

    @bot.on(events.NewMessage(pattern="/start"))
    async def cmd_start(event: events.NewMessage.Event):
        _conv_state.pop(_key(event), None)          # reset any ongoing flow
        await event.respond(
            "**🛠  TG Admin Dashboard**\n\nSelect an option:",
            buttons=_main_menu_keyboard(),
        )
        raise events.StopPropagation

    # ── Inline callback router ─────────────────────────────────────────────────

    @bot.on(events.CallbackQuery())
    async def callback_router(event: events.CallbackQuery.Event):
        data: str = event.data.decode()

        # ── Main menu navigation ──────────────────────────────────────────────
        if data == "menu:back":
            await _go_main_menu(event)

        elif data == "menu:view":
            await _cb_view_accounts(event, db)

        elif data == "menu:add":
            await _cb_add_start(event)

        elif data == "menu:remove":
            await _cb_remove_list(event, db)

        # ── Remove flow ───────────────────────────────────────────────────────
        elif data.startswith("rm_select:"):
            uid = int(data.split(":")[1])
            await _cb_rm_confirm_prompt(event, db, uid)

        elif data.startswith("rm_confirm:"):
            uid = int(data.split(":")[1])
            await _cb_rm_execute(event, db, workers, uid)

        # ── Account sub-menu ──────────────────────────────────────────────────
        elif data.startswith("acc:"):
            uid = int(data.split(":")[1])
            await _cb_account_menu(event, db, uid)

        # ── Account actions (stub handlers) ──────────────────────────────────
        elif data.startswith("act:"):
            await _cb_action_dispatch(event, workers, data)

        await event.answer()      # always ack the callback

    # ── Plain-text message handler (conversation steps) ───────────────────────

    @bot.on(events.NewMessage(func=lambda e: e.is_private and not e.via_bot_id))
    async def text_handler(event: events.NewMessage.Event):
        key = _key(event)
        state = _conv_state.get(key)
        if not state:
            return                # nothing pending

        if state["step"] == STEP_AWAIT_SESSION:
            await _handle_session_input(event, db, workers, key, state)

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helpers (defined as closures so they share the outer scope)
    # ──────────────────────────────────────────────────────────────────────────

    async def _go_main_menu(event: Any):
        await event.edit(
            "**🛠  TG Admin Dashboard**\n\nSelect an option:",
            buttons=_main_menu_keyboard(),
        )

    async def _cb_view_accounts(event: Any, db: Database):
        accounts = await db.get_all_accounts()
        if not accounts:
            await event.edit(
                "📭  No accounts stored yet.\n\nUse **Add Account** to get started.",
                buttons=[[Button.inline("⬅️  Back", b"menu:back")]],
            )
            return
        await event.edit(
            f"📋  **Managed Accounts** ({len(accounts)} total)\n\nTap to open:",
            buttons=_accounts_keyboard(accounts),
        )

    async def _cb_add_start(event: Any):
        key = _key(event)
        _conv_state[key] = {"step": STEP_AWAIT_SESSION}
        await event.edit(
            "➕  **Add Account**\n\n"
            "Send the **StringSession** for the account you want to add.\n\n"
            "⚠️  Make sure the session is from a fresh, authorised Telethon client.\n\n"
            "Type /cancel to abort.",
            buttons=[[Button.inline("❌  Cancel", b"menu:back")]],
        )

    async def _cb_remove_list(event: Any, db: Database):
        accounts = await db.get_all_accounts()
        if not accounts:
            await event.edit(
                "📭  No accounts to remove.",
                buttons=[[Button.inline("⬅️  Back", b"menu:back")]],
            )
            return
        await event.edit(
            "➖  **Remove Account**\n\nSelect the account to remove:",
            buttons=_remove_list_keyboard(accounts),
        )

    async def _cb_rm_confirm_prompt(event: Any, db: Database, uid: int):
        account = await db.get_account(uid)
        if not account:
            await event.answer("Account not found.", alert=True)
            return
        await event.edit(
            f"⚠️  **Confirm Removal**\n\n"
            f"Account: **{account['name']}** (@{account['username'] or 'N/A'})\n"
            f"User ID: `{uid}`\n\n"
            f"This will disconnect the session and delete it from the database.",
            buttons=_confirm_remove_keyboard(uid),
        )

    async def _cb_rm_execute(
        event: Any, db: Database, workers: WorkerMap, uid: int
    ):
        await workers.remove_worker(uid)
        deleted = await db.remove_account(uid)
        if deleted:
            await event.edit(
                f"✅  Account `{uid}` has been removed successfully.",
                buttons=[[Button.inline("⬅️  Back to Menu", b"menu:back")]],
            )
        else:
            await event.edit(
                "❌  Account not found in database.",
                buttons=[[Button.inline("⬅️  Back to Menu", b"menu:back")]],
            )

    async def _cb_account_menu(event: Any, db: Database, uid: int):
        account = await db.get_account(uid)
        if not account:
            await event.answer("Account not found.", alert=True)
            return
        worker = workers.get(uid)
        status = "🟢 Connected" if worker else "🔴 Offline"
        await event.edit(
            f"👤  **{account['name']}**\n"
            f"@{account['username'] or 'N/A'}  |  `{uid}`\n"
            f"Status: {status}\n\n"
            "Choose an action:",
            buttons=_account_action_keyboard(uid),
        )

    async def _cb_action_dispatch(event: Any, workers: WorkerMap, data: str):
        """
        Stub dispatcher for per-account actions.
        Each action should be implemented in its own plugin file and
        imported/registered here or in main.py.
        """
        parts = data.split(":")          # e.g. ["act", "bc_gc", "123456"]
        action = parts[1] if len(parts) > 1 else "unknown"
        uid    = int(parts[2]) if len(parts) > 2 else 0

        worker = workers.get(uid)
        if not worker:
            await event.answer(
                "⚠️  This account is not connected.", alert=True
            )
            return

        action_labels = {
            "bc_gc":       "Broadcast GC",
            "bc_dm":       "Broadcast DM",
            "leave_muted": "Leave Muted Groups",
            "join_groups": "Start Joining Groups",
            "profile":     "Profile Management",
        }
        label = action_labels.get(action, action)
        # ── Replace this stub with a real plugin call ──────────────────────
        await event.answer(f"🚧  {label} — coming soon.", alert=True)

    async def _handle_session_input(
        event: events.NewMessage.Event,
        db: Database,
        workers: WorkerMap,
        key: tuple,
        state: dict,
    ):
        """Validate the pasted StringSession and persist it."""
        raw = event.raw_text.strip()

        if raw == "/cancel":
            _conv_state.pop(key, None)
            await event.respond(
                "❌  Cancelled.",
                buttons=_main_menu_keyboard(),
            )
            return

        status_msg = await event.respond("⏳  Validating session, please wait…")

        try:
            # Attempt to connect with the provided session
            temp_client = TelegramClient(
                StringSession(raw),
                workers._api_id,
                workers._api_hash,
            )
            await temp_client.connect()

            if not await temp_client.is_user_authorized():
                raise PermissionError("Session is not authorised.")

            me = await temp_client.get_me()
            await temp_client.disconnect()

        except (ValueError, PermissionError, AuthKeyError) as exc:
            _conv_state.pop(key, None)
            await status_msg.edit(
                f"❌  **Invalid session.**\n\n`{exc}`\n\nReturning to main menu.",
                buttons=_main_menu_keyboard(),
            )
            return

        except Exception as exc:
            _conv_state.pop(key, None)
            logger.exception("Unexpected error during session validation")
            await status_msg.edit(
                f"❌  Unexpected error: `{exc}`",
                buttons=_main_menu_keyboard(),
            )
            return

        # Persist to DB
        await db.add_account(
            user_id=me.id,
            name=f"{me.first_name or ''} {me.last_name or ''}".strip(),
            username=me.username,
            session_string=raw,
        )

        # Spin up live worker
        try:
            await workers.add_worker(me.id, raw)
        except Exception as exc:
            logger.warning("Worker start failed after DB save: %s", exc)

        _conv_state.pop(key, None)
        await status_msg.edit(
            f"✅  **Account added!**\n\n"
            f"Name: **{me.first_name} {me.last_name or ''}**\n"
            f"Username: @{me.username or 'N/A'}\n"
            f"User ID: `{me.id}`",
            buttons=_main_menu_keyboard(),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────────────────────

def _key(event: Any) -> tuple:
    """Unique key per (chat, sender) for conversation state isolation."""
    return (event.chat_id, event.sender_id)
