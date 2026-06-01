"""
plugins/join_groups.py

Handles the full "Start Joining Groups" flow:
  - Manage source channels per account (add / remove)
  - Run join task in background (non-blocking)
  - Track joined links per account in MongoDB
  - Live progress editing via inline message

Conversation states for adding a source channel are tracked
in _join_conv_state keyed by (chat_id, sender_id).
"""

import re
import asyncio
import logging
from typing import Optional

from telethon import TelegramClient, events, Button
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import (
    FloodWaitError,
    UserAlreadyParticipantError,
    InviteRequestSentError,
)

from core.database import Database
from core.worker_map import WorkerMap

logger = logging.getLogger(__name__)

# ── Conversation state ────────────────────────────────────────────────────────
_join_conv_state: dict[tuple, dict] = {}

STEP_IDLE             = "idle"
STEP_AWAIT_CHANNEL_ID = "awaiting_channel_id"

# ── Active join tasks: user_id → asyncio.Task  (prevent double-starts) ───────
_active_tasks: dict[int, asyncio.Task] = {}

JOIN_DELAY = 12   # seconds between joins (safe default)


# ─────────────────────────────────────────────────────────────────────────────
# Keyboards
# ─────────────────────────────────────────────────────────────────────────────

def _join_menu_keyboard(uid: int, source_count: int, joined_count: int) -> list:
    uid_b = str(uid).encode()
    running = uid in _active_tasks and not _active_tasks[uid].done()
    start_label = "⏳  Task Running…" if running else "▶️  Start Joining"
    return [
        [Button.inline(
            f"📡  Manage Sources  ({source_count} configured)",
            f"jn:sources:{uid}".encode()
        )],
        [Button.inline(
            f"▶️  {start_label}",
            f"jn:start:{uid}".encode()
        )],
        [Button.inline(
            f"🗑  Clear Join History  ({joined_count} links)",
            f"jn:clear_history:{uid}".encode()
        )],
        [Button.inline("⬅️  Back", f"acc:{uid}".encode())],
    ]


def _sources_keyboard(uid: int, channels: list[int]) -> list:
    rows = []
    for ch in channels:
        rows.append([Button.inline(
            f"❌  Remove  {ch}",
            f"jn:rm_src:{uid}:{ch}".encode()
        )])
    rows.append([Button.inline("➕  Add Source Channel", f"jn:add_src:{uid}".encode())])
    rows.append([Button.inline("⬅️  Back", f"jn:menu:{uid}".encode())])
    return rows


def _confirm_clear_keyboard(uid: int) -> list:
    return [
        [
            Button.inline("✅  Yes, clear", f"jn:clear_confirm:{uid}".encode()),
            Button.inline("❌  Cancel",     f"jn:menu:{uid}".encode()),
        ]
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Link extractor
# ─────────────────────────────────────────────────────────────────────────────

def _extract_link(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    match = re.search(r"(https?://t\.me/[^\s]+)", text)
    return match.group(1) if match else None


# ─────────────────────────────────────────────────────────────────────────────
# Core join worker  (runs as background asyncio Task)
# ─────────────────────────────────────────────────────────────────────────────

async def _run_join_task(
    worker: TelegramClient,
    user_id: int,
    source_channels: list[int],
    db: Database,
    progress_msg,          # Telethon Message object to edit
):
    """
    Iterate all source channels, extract invite links,
    join groups not yet joined by this account.
    Skips broadcast channels (announcements-only).
    """
    total_done  = 0
    total_skip  = 0
    total_fail  = 0

    for src_channel in source_channels:
        try:
            messages = []
            async for m in worker.iter_messages(src_channel):
                messages.append(m)

            for m in messages:
                link = _extract_link(m.text)
                if not link:
                    continue

                # skip if already joined by this account
                if await db.is_link_joined(user_id, link):
                    total_skip += 1
                    continue

                try:
                    # ── PRIVATE invite links ──────────────────────────────
                    if "joinchat/" in link or "/+" in link:
                        hash_ = link.split("/")[-1].lstrip("+")
                        try:
                            result = await worker(ImportChatInviteRequest(hash_))
                            chats = result.chats
                            if chats and getattr(chats[0], "broadcast", False):
                                # broadcast channel — skip
                                total_skip += 1
                                continue
                        except InviteRequestSentError:
                            pass  # request sent, still mark as processed

                    # ── PUBLIC links ──────────────────────────────────────
                    else:
                        username = link.rstrip("/").split("/")[-1]
                        try:
                            entity = await worker.get_entity(username)
                        except Exception:
                            total_fail += 1
                            continue

                        if getattr(entity, "broadcast", False):
                            total_skip += 1
                            continue

                        try:
                            await worker(JoinChannelRequest(username))
                        except UserAlreadyParticipantError:
                            pass

                    # mark as joined in MongoDB
                    await db.mark_link_joined(user_id, link)
                    total_done += 1

                    # update progress
                    try:
                        await progress_msg.edit(
                            f"⏳  **Join Task Running**\n\n"
                            f"✅  Joined:  `{total_done}`\n"
                            f"⏭️  Skipped: `{total_skip}`\n"
                            f"❌  Failed:  `{total_fail}`\n\n"
                            f"_Delay: {JOIN_DELAY}s per join_"
                        )
                    except Exception:
                        pass

                    await asyncio.sleep(JOIN_DELAY)

                except FloodWaitError as e:
                    logger.warning("FloodWait %ds for account %s", e.seconds, user_id)
                    try:
                        await progress_msg.edit(
                            f"⚠️  FloodWait — sleeping `{e.seconds}s`\n\n"
                            f"✅  Joined so far: `{total_done}`"
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(e.seconds)

                except Exception as e:
                    logger.warning("Join failed for %s: %s", link, e)
                    total_fail += 1

        except Exception as e:
            logger.error("Error reading source channel %s: %s", src_channel, e)

    # final status
    try:
        total_joined_ever = await db.get_joined_count(user_id)
        await progress_msg.edit(
            f"✅  **Join Task Complete**\n\n"
            f"This session:\n"
            f"  Joined:  `{total_done}`\n"
            f"  Skipped: `{total_skip}` (already done)\n"
            f"  Failed:  `{total_fail}`\n\n"
            f"Total joined ever (this account): `{total_joined_ever}`"
        )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Plugin registration
# ─────────────────────────────────────────────────────────────────────────────

def register(bot: TelegramClient, db: Database, workers: WorkerMap):

    # ── Callback router for all jn: prefixed callbacks ───────────────────────

    @bot.on(events.CallbackQuery(pattern=b"jn:.*"))
    async def join_callback_router(event: events.CallbackQuery.Event):
        data = event.data.decode()
        parts = data.split(":")   # e.g. ["jn", "menu", "12345"]

        action = parts[1] if len(parts) > 1 else ""
        uid    = int(parts[2]) if len(parts) > 2 else 0

        # ── Open join menu ────────────────────────────────────────────────────
        if action == "menu":
            await _show_join_menu(event, db, workers, uid)

        # ── Manage sources ────────────────────────────────────────────────────
        elif action == "sources":
            await _show_sources(event, db, uid)

        elif action == "add_src":
            await _prompt_add_source(event, uid)

        elif action == "rm_src":
            channel_id = int(parts[3]) if len(parts) > 3 else 0
            await _remove_source(event, db, uid, channel_id)

        # ── Start join task ───────────────────────────────────────────────────
        elif action == "start":
            await _start_join(event, db, workers, uid)

        # ── Clear history ─────────────────────────────────────────────────────
        elif action == "clear_history":
            await _confirm_clear_history(event, uid)

        elif action == "clear_confirm":
            await _execute_clear_history(event, db, uid)

        await event.answer()

    # ── Text handler for channel ID input ─────────────────────────────────────

    @bot.on(events.NewMessage(func=lambda e: e.is_private and not e.via_bot_id))
    async def join_text_handler(event: events.NewMessage.Event):
        key = (event.chat_id, event.sender_id)
        state = _join_conv_state.get(key)
        if not state:
            return

        if state["step"] == STEP_AWAIT_CHANNEL_ID:
            await _handle_channel_id_input(event, db, key, state)

    # ── Internal handlers ─────────────────────────────────────────────────────

    async def _show_join_menu(event, db: Database, workers: WorkerMap, uid: int):
        sources = await db.get_source_channels(uid)
        joined  = await db.get_joined_count(uid)
        account = await db.get_account(uid)
        name    = account["name"] if account else str(uid)

        await event.edit(
            f"🔗  **Join Groups** — {name}\n\n"
            f"Source channels: `{len(sources)}`\n"
            f"Total joined (this account): `{joined}`",
            buttons=_join_menu_keyboard(uid, len(sources), joined),
        )

    async def _show_sources(event, db: Database, uid: int):
        sources = await db.get_source_channels(uid)
        text = (
            f"📡  **Source Channels** for `{uid}`\n\n"
            + (
                "\n".join(f"• `{ch}`" for ch in sources)
                if sources else
                "_No sources configured yet._"
            )
            + "\n\nAdd channel ID (e.g. `-1001234567890`) or username."
        )
        await event.edit(text, buttons=_sources_keyboard(uid, sources))

    async def _prompt_add_source(event, uid: int):
        key = (event.chat_id, event.sender_id)
        _join_conv_state[key] = {"step": STEP_AWAIT_CHANNEL_ID, "uid": uid}
        await event.edit(
            "📡  **Add Source Channel**\n\n"
            "Send the channel **ID** (e.g. `-1001234567890`) or **@username**.\n\n"
            "Type /cancel to abort.",
            buttons=[[Button.inline("❌  Cancel", f"jn:sources:{uid}".encode())]],
        )

    async def _remove_source(event, db: Database, uid: int, channel_id: int):
        removed = await db.remove_source_channel(uid, channel_id)
        if removed:
            await event.answer(f"Removed {channel_id}", alert=False)
        else:
            await event.answer("Not found.", alert=True)
        # refresh sources view
        await _show_sources(event, db, uid)

    async def _start_join(event, db: Database, workers: WorkerMap, uid: int):
        # block if already running
        if uid in _active_tasks and not _active_tasks[uid].done():
            await event.answer("⚠️  Join task is already running.", alert=True)
            return

        worker = workers.get(uid)
        if not worker:
            await event.answer("❌  Account is not connected.", alert=True)
            return

        sources = await db.get_source_channels(uid)
        if not sources:
            await event.answer(
                "⚠️  No source channels configured.\nAdd at least one source first.",
                alert=True
            )
            return

        # post progress message (editable)
        progress_msg = await event.respond(
            "⏳  **Join Task Starting…**\n\n"
            f"Sources: `{len(sources)}`\n"
            "Fetching messages…"
        )

        # fire background task
        task = asyncio.create_task(
            _run_join_task(worker, uid, sources, db, progress_msg)
        )
        _active_tasks[uid] = task

        # cleanup handle after done
        def _task_done(t: asyncio.Task):
            _active_tasks.pop(uid, None)
            if t.exception():
                logger.error("Join task error for %s: %s", uid, t.exception())

        task.add_done_callback(_task_done)

        await event.answer("✅  Join task started!", alert=False)

    async def _confirm_clear_history(event, uid: int):
        count = await db.get_joined_count(uid)
        await event.edit(
            f"⚠️  **Clear Join History**\n\n"
            f"This will delete `{count}` link records for this account.\n"
            f"The account will **re-join** all links on next run.\n\n"
            f"Are you sure?",
            buttons=_confirm_clear_keyboard(uid),
        )

    async def _execute_clear_history(event, db: Database, uid: int):
        deleted = await db.clear_joined_links(uid)
        await event.edit(
            f"✅  Cleared `{deleted}` joined link records.\n\n"
            f"This account will re-join everything on next run.",
            buttons=[[Button.inline("⬅️  Back", f"jn:menu:{uid}".encode())]],
        )

    async def _handle_channel_id_input(
        event: events.NewMessage.Event,
        db: Database,
        key: tuple,
        state: dict,
    ):
        uid = state["uid"]
        raw = event.raw_text.strip()

        if raw == "/cancel":
            _join_conv_state.pop(key, None)
            await event.respond(
                "❌  Cancelled.",
                buttons=[[Button.inline("⬅️  Back", f"jn:sources:{uid}".encode())]],
            )
            return

        # resolve: numeric ID or @username
        channel_id: Optional[int] = None
        try:
            channel_id = int(raw)
        except ValueError:
            # treat as username — resolve via bot client
            username = raw.lstrip("@")
            try:
                entity = await bot.get_entity(username)
                channel_id = entity.id
                # supergroup/channel IDs need -100 prefix
                if channel_id > 0:
                    channel_id = int(f"-100{channel_id}")
            except Exception as e:
                _join_conv_state.pop(key, None)
                await event.respond(
                    f"❌  Could not resolve `{raw}`\n`{e}`",
                    buttons=[[Button.inline("⬅️  Back", f"jn:sources:{uid}".encode())]],
                )
                return

        added = await db.add_source_channel(uid, channel_id)
        _join_conv_state.pop(key, None)

        if added:
            await event.respond(
                f"✅  Source `{channel_id}` added.",
                buttons=[[Button.inline("⬅️  Back to Sources", f"jn:sources:{uid}".encode())]],
            )
        else:
            await event.respond(
                f"ℹ️  `{channel_id}` was already in the list.",
                buttons=[[Button.inline("⬅️  Back to Sources", f"jn:sources:{uid}".encode())]],
                      )
