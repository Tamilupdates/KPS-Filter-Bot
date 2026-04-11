"""
filter_keywords.py
──────────────────
Admin-only commands for managing the FILTER_KEYWORDS block-list in MongoDB.

Commands (send in bot PM or any group where the bot is present)
--------
/listkw             – Show all currently blocked keywords
/addkw  <keyword>  – Add one or more comma-separated keywords
                     ↳ If already-indexed files match, a confirm-delete prompt appears
/removekw <keyword> – Remove one keyword
"""

import logging
import base64
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from info import ADMINS
from database.filter_keywords_db import (
    get_all_keywords,
    add_keyword,
    remove_keyword,
    count_files_with_keyword,
    delete_files_with_keyword,
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────

def _encode_kw(kw: str) -> str:
    """Base64-encode a keyword so it is safe inside callback_data."""
    return base64.urlsafe_b64encode(kw.encode()).decode().rstrip("=")


def _decode_kw(token: str) -> str:
    """Decode a base64-encoded keyword from callback_data."""
    pad = (4 - len(token) % 4) % 4
    return base64.urlsafe_b64decode(token + "=" * pad).decode()


def _confirm_buttons(kw_encoded: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Delete Files", callback_data=f"kwdel#{kw_encoded}"),
            InlineKeyboardButton("❌ No, Keep Files",   callback_data="kwkeep"),
        ]
    ])


# ──────────────────────────────────────────────────────────────────
# /listkw  – view all blocked keywords (admins only)
# ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("listkw") & filters.user(ADMINS) & filters.incoming)
async def list_keywords(client: Client, message: Message):
    """List all blocked filter keywords stored in MongoDB."""
    try:
        keywords = await get_all_keywords()
    except Exception as e:
        logger.exception(e)
        await message.reply_text(
            f"<b>❌ Error fetching keywords:</b>\n<code>{e}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    if not keywords:
        await message.reply_text(
            "<b>🔍 No blocked keywords found in the database.</b>\n\n"
            "Use <code>/addkw keyword1, keyword2</code> to add some.",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    numbered = "\n".join(
        f"  <b>{i}.</b> <code>{kw}</code>"
        for i, kw in enumerate(keywords, start=1)
    )
    text = (
        f"<b>🚫 <u>Blocked Keywords</u>  ({len(keywords)} total)</b>\n\n"
        f"{numbered}\n\n"
        "<i>Use /addkw or /removekw to manage this list.</i>"
    )
    await message.reply_text(text, parse_mode=enums.ParseMode.HTML)


# ──────────────────────────────────────────────────────────────────
# /addkw  – add one or more keywords (admins only)
# ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("addkw") & filters.user(ADMINS) & filters.incoming)
async def add_keyword_cmd(client: Client, message: Message):
    """
    Add one or more comma-separated keywords to the block list.
    After adding, checks if any already-indexed files match the keyword.
    If so, shows a delete confirmation prompt.
    """
    if len(message.command) < 2:
        await message.reply_text(
            "<b>Usage:</b>\n"
            "  <code>/addkw keyword</code>\n"
            "  <code>/addkw keyword1, keyword2, keyword3</code>\n\n"
            "<i>Use /listkw to see the full list.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    raw = message.text.split(None, 1)[1]
    keywords = [kw.strip() for kw in raw.split(",") if kw.strip()]

    added = []
    already = []

    for kw in keywords:
        try:
            result = await add_keyword(kw)
            if result:
                added.append(kw.lower())
            else:
                already.append(kw.lower())
        except Exception as e:
            logger.exception(e)
            await message.reply_text(
                f"<b>❌ Error adding <code>{kw}</code>:</b> <code>{e}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            return

    # ── Report what was added / skipped ──────────────────────────
    parts = []
    if added:
        kw_list = "\n".join(f"  ✅ <code>{k}</code>" for k in added)
        parts.append(
            f"<b>Added successfully:</b>\n{kw_list}\n\n"
            "<i>Use /listkw or /removekw to manage this list.</i>"
        )
    if already:
        kw_list = "\n".join(f"  ⚠️ <code>{k}</code>" for k in already)
        parts.append(
            f"<b>Already in list (skipped):</b>\n{kw_list}\n\n"
            "<i>Use /listkw or /removekw to manage this list.</i>"
        )

    if parts:
        await message.reply_text("\n\n".join(parts), parse_mode=enums.ParseMode.HTML)

    # ── For each newly-added keyword, check existing indexed files ─
    for kw in added:
        try:
            count = await count_files_with_keyword(kw)
        except Exception as e:
            logger.exception(e)
            continue

        if count > 0:
            kw_encoded = _encode_kw(kw)
            await message.reply_text(
                f"⚠️ <b>Found <u>{count} already-indexed file(s)</u> containing keyword:</b> "
                f"<code>{kw}</code>\n\n"
                f"Do you want to <b>delete these {count} file(s)</b> from the database now?",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=_confirm_buttons(kw_encoded),
            )
        # If count == 0 → silently skip, no prompt needed


# ──────────────────────────────────────────────────────────────────
# Callback: ✅ Yes – delete matching indexed files
# ──────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^kwdel#") & filters.user(ADMINS))
async def cb_delete_files(client: Client, query: CallbackQuery):
    """Handle 'Yes, Delete Files' button after adding a keyword."""
    await query.answer()

    kw_encoded = query.data.split("#", 1)[1]
    try:
        kw = _decode_kw(kw_encoded)
    except Exception:
        await query.message.edit_text(
            "<b>❌ Invalid callback data. Please try again.</b>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    await query.message.edit_text(
        f"⏳ <b>Deleting files containing:</b> <code>{kw}</code>…",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        deleted = await delete_files_with_keyword(kw)
    except Exception as e:
        logger.exception(e)
        await query.message.edit_text(
            f"<b>❌ Error during deletion:</b>\n<code>{e}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    await query.message.edit_text(
        f"✅ <b>Deleted {deleted} file(s) Successfully,</b> "
        f"containing keyword <code>{kw}</code> from the database.\n\n"
        "<i>These files will no longer be indexed unless re-uploaded.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ──────────────────────────────────────────────────────────────────
# Callback: ❌ No – keep the files, just dismiss the prompt
# ──────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^kwkeep$") & filters.user(ADMINS))
async def cb_keep_files(client: Client, query: CallbackQuery):
    """Handle 'No, Keep Files' button after adding a keyword."""
    await query.answer("Keyword added. Existing files kept.", show_alert=False)
    await query.message.edit_text(
        "✅ <b>Keyword added.</b> Existing indexed files were <b>kept</b>.\n\n"
        "<i>Future uploads containing this keyword will be blocked automatically.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ──────────────────────────────────────────────────────────────────
# /removekw  – remove a keyword (admins only)
# ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("removekw") & filters.user(ADMINS) & filters.incoming)
async def remove_keyword_cmd(client: Client, message: Message):
    """
    Remove a keyword from the block list.

    Usage:
        /removekw PreDVD

    <i>Use /listkw to see the full list.</i>
    """
    if len(message.command) < 2:
        await message.reply_text(
            "<b>Usage:</b>\n"
            "  <code>/removekw keyword</code>\n"
            "  <code>/removekw keyword1, keyword2, keyword3</code>\n\n"
            "<i>Use /listkw to see the full list.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    keyword = message.text.split(None, 1)[1].strip()

    try:
        removed = await remove_keyword(keyword)
    except Exception as e:
        logger.exception(e)
        await message.reply_text(
            f"<b>❌ Error:</b> <code>{e}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    if removed:
        await message.reply_text(
            f"✅ <b>Keyword removed:</b> <code>{keyword.lower()}</code>\n\n"
            "The block list has been updated immediately.\n\n"
            "<i>Use /listkw or /addkw to manage this list.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
    else:
        await message.reply_text(
            f"❌ <b>Keyword not found:</b> <code>{keyword.lower()}</code>\n\n"
            "Use <code>/listkw</code> to see all blocked keywords.",
            parse_mode=enums.ParseMode.HTML,
        )
