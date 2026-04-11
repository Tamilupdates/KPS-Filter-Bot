"""
size_filter.py
──────────────
Admin-only commands for managing file-size filters and bulk-deleting
already-indexed files by size.

Commands
--------
/listsize               – Show all active size filter rules
/setminsize <size>      – Set minimum file size; blocks below + cleans existing (one command)
/addsize  <expr>        – Add a size filter rule (advanced, supports operators)
/removesize <expr>      – Remove a size filter rule  (alias: /deletesize)
/deletesize <expr>      – Same as /removesize
/deletefiles <expr>     – Bulk-delete already-indexed files matching a size
                          expression, with Yes/No confirmation

Size expression formats (all case-insensitive):
  100MB   0.5GB   512KB   1.2GB     → exact  match
  <100MB  <=1GB   >512KB  >=2GB     → comparison match
  =100MB                            → explicit exact match

Examples:
  /setminsize 100KB      → block all future files < 100KB + delete existing < 100KB
  /addsize <100MB        → skip files smaller than 100 MB during indexing
  /addsize =0.12MB       → skip files of exactly 0.12 MB
  /deletefiles <1MB      → delete all indexed files smaller than 1 MB
"""

import logging
import base64
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from info import ADMINS
from utils import get_size
from database.size_filter_db import (
    get_all_size_rules,
    add_size_rule,
    remove_size_rule,
    count_indexed_files_by_size,
    delete_indexed_files_by_size,
    parse_size_string,
)

logger = logging.getLogger(__name__)

_OP_DISPLAY = {
    "eq":  "=",
    "lt":  "<",
    "lte": "≤",
    "gt":  ">",
    "gte": "≥",
}


# ──────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────

def _enc(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")

def _dec(t: str) -> str:
    # (4 - n%4) % 4 ensures 0 extra '=' when already aligned (avoids invalid base64)
    pad = (4 - len(t) % 4) % 4
    return base64.urlsafe_b64decode(t + "=" * pad).decode()

def _confirm_btns(expr_enc: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, Delete", callback_data=f"szdel#{expr_enc}"),
        InlineKeyboardButton("❌ No, Cancel",  callback_data="szkeep"),
    ]])


# ──────────────────────────────────────────────────────────────────
# /setminsize  – one-command minimum size setup (admin only)
# ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("setminsize") & filters.user(ADMINS) & filters.incoming)
async def setminsize_cmd(client: Client, message: Message):
    """
    Set a minimum file size. Files below this size will:
      1. Be blocked from future indexing immediately
      2. If already indexed — show count and ask to delete them

    Usage:
        /setminsize 100KB   → block & clean up all files below 100 KB
        /setminsize 1MB     → block & clean up all files below 1 MB
        /setminsize 0.5GB   → block & clean up all files below 0.5 GB
    """
    if len(message.command) < 2:
        await message.reply_text(
            "<b>📏 Set Minimum File Size</b>\n\n"
            "<b>Usage:</b>\n"
            "  <code>/setminsize 100KB</code>  – block files below 100 KB\n"
            "  <code>/setminsize 1MB</code>    – block files below 1 MB\n"
            "  <code>/setminsize 0.5GB</code>  – block files below 0.5 GB\n\n"
            "<i>This will:</i>\n"
            "🔹 <i>Block future files below this size from being indexed</i>\n"
            "🔹 <i>Count any already-indexed files below this size</i>\n"
            "🔹 <i>Ask you to delete them if found</i>\n\n"
            "<i>Units: KB, MB, GB, TB (case-insensitive)</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    raw_input = message.text.split(None, 1)[1].strip()
    # Strip any accidental operator — always treats as "less than"
    raw_clean = raw_input.lstrip("<>=").strip()
    raw_lt = f"<{raw_clean}"

    parsed = parse_size_string(raw_lt)
    if not parsed:
        await message.reply_text(
            f"❌ <b>Cannot parse:</b> <code>{raw_input}</code>\n\n"
            "Use formats like: <code>100KB</code>, <code>1MB</code>, <code>0.5GB</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    _, _, label = parsed

    # ─ Step 1: Add the filter rule ───────────────────────────────────
    try:
        ok, result = await add_size_rule(raw_lt)
    except Exception as e:
        logger.exception(e)
        await message.reply_text(f"<b>❌ Error saving rule:</b> <code>{e}</code>",
                                  parse_mode=enums.ParseMode.HTML)
        return

    if ok:
        rule_msg = f"✅ <b>Minimum size set:</b> files <b>below {label}</b> will be blocked from indexing."
    else:
        rule_msg = f"⚠️ <b>Note:</b> Rule <code>{label}</code> was already active."

    await message.reply_text(rule_msg, parse_mode=enums.ParseMode.HTML)

    # ─ Step 2: Count existing indexed files below this size ─────────────
    try:
        count, lbl = await count_indexed_files_by_size(raw_lt)
    except Exception as e:
        logger.exception(e)
        await message.reply_text(
            f"<b>⚠️ Rule saved, but could not check existing files:</b>\n<code>{e}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    if count == 0:
        await message.reply_text(
            f"✅ <b>No existing indexed files found below {lbl}</b>\n\n"
            "<i>All future files below this size will be skipped automatically.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    # ─ Step 3: Confirm delete ─────────────────────────────────────────
    await message.reply_text(
        f"⚠️ <b>Found <u>{count} already-indexed file(s)</u> below <code>{lbl}</code></b>\n\n"
        f"Do you want to <b>permanently delete these {count} file(s)</b> from the database?",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=_confirm_btns(_enc(raw_lt)),
    )


# ──────────────────────────────────────────────────────────────────
# /listsize
# ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("listsize") & filters.user(ADMINS) & filters.incoming)
async def list_size_rules(client: Client, message: Message):
    """Show all active size filter rules."""
    try:
        rules = await get_all_size_rules()
    except Exception as e:
        logger.exception(e)
        await message.reply_text(f"<b>❌ Error:</b> <code>{e}</code>", parse_mode=enums.ParseMode.HTML)
        return

    if not rules:
        await message.reply_text(
            "<b>📏 No size filter rules configured.</b>\n\n"
            "Use <code>/addsize &lt;100MB</code> to add one.\n\n"
            "<i>Supported: =, &lt;, &lt;=, &gt;, &gt;= with KB, MB, GB, TB</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    lines = "\n".join(
        f"  <b>{i}.</b> <code>{_OP_DISPLAY[r['operator']]} {r['label'].lstrip('<>=').strip()}</code>"
        for i, r in enumerate(rules, 1)
    )
    await message.reply_text(
        f"<b>📏 <u>Size Filter Rules</u>  ({len(rules)} total)</b>\n\n"
        f"{lines}\n\n"
        "<i>Files matching these rules are skipped during indexing.\n"
        "Use /addsize or /deletesize to manage.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ──────────────────────────────────────────────────────────────────
# /addsize
# ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("addsize") & filters.user(ADMINS) & filters.incoming)
async def add_size_cmd(client: Client, message: Message):
    """Add a size filter rule."""
    if len(message.command) < 2:
        await message.reply_text(
            "<b>Usage:</b>\n"
            "  <code>/addsize &lt;100MB</code>    – skip files smaller than 100 MB\n"
            "  <code>/addsize &gt;=1GB</code>     – skip files 1 GB or larger\n"
            "  <code>/addsize =0.12MB</code>  – skip files of exactly 0.12 MB\n"
            "  <code>/addsize 512KB</code>    – skip files of exactly 512 KB\n\n"
            "<i>Units: KB, MB, GB, TB (case-insensitive)</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    raw = message.text.split(None, 1)[1].strip()
    try:
        ok, result = await add_size_rule(raw)
    except Exception as e:
        logger.exception(e)
        await message.reply_text(f"<b>❌ Error:</b> <code>{e}</code>", parse_mode=enums.ParseMode.HTML)
        return

    if not ok:
        await message.reply_text(result, parse_mode=enums.ParseMode.HTML)
        return

    label = result
    await message.reply_text(
        f"✅ <b>Size filter added:</b> <code>{label}</code>\n\n"
        "<i>New files matching this size will be skipped during indexing.</i>\n\n"
        "<i>Use /listsize or /deletesize to manage.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    # Check already-indexed files
    try:
        count, lbl = await count_indexed_files_by_size(raw)
        if count > 0:
            await message.reply_text(
                f"⚠️ <b>Found <u>{count} already-indexed file(s)</u> matching:</b> <code>{lbl}</code>\n\n"
                f"Do you want to <b>delete these {count} file(s)</b> from the database now?",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=_confirm_btns(_enc(raw)),
            )
    except Exception as e:
        logger.exception(e)


# ──────────────────────────────────────────────────────────────────
# /removesize
# ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command(["removesize", "deletesize"]) & filters.user(ADMINS) & filters.incoming)
async def remove_size_cmd(client: Client, message: Message):
    """Remove a size filter rule. Alias: /deletesize"""
    if len(message.command) < 2:
        await message.reply_text(
            "<b>Usage:</b>\n"
            "  <code>/deletesize &lt;100MB</code>\n"
            "  <code>/deletesize &gt;=1GB</code>\n\n"
            "Use <code>/listsize</code> to see active rules.",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    raw = message.text.split(None, 1)[1].strip()
    try:
        ok, result = await remove_size_rule(raw)
    except Exception as e:
        logger.exception(e)
        await message.reply_text(f"<b>❌ Error:</b> <code>{e}</code>", parse_mode=enums.ParseMode.HTML)
        return

    if ok:
        await message.reply_text(
            f"✅ <b>Size filter removed:</b> <code>{result}</code>\n\n"
            "<i>Use /listsize or /addsize to manage.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
    else:
        await message.reply_text(result, parse_mode=enums.ParseMode.HTML)


# ──────────────────────────────────────────────────────────────────
# /deletefiles  – bulk-delete indexed files by size
# ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("deletefiles") & filters.user(ADMINS) & filters.incoming)
async def deletefiles_cmd(client: Client, message: Message):
    """
    Bulk-delete already-indexed files matching a size expression.

    Usage:
        /deletefiles <100MB     → delete all files smaller than 100 MB
        /deletefiles >=1GB      → delete all files 1 GB or larger
        /deletefiles =0.12MB    → delete files of exactly 0.12 MB
        /deletefiles 512KB      → delete files of exactly 512 KB
    """
    if len(message.command) < 2:
        await message.reply_text(
            "<b>Usage:</b>\n"
            "  <code>/deletefiles &lt;100MB</code>   – delete files smaller than 100 MB\n"
            "  <code>/deletefiles &gt;=1GB</code>    – delete files 1 GB or larger\n"
            "  <code>/deletefiles =0.12MB</code> – delete files of exactly 0.12 MB\n"
            "  <code>/deletefiles 512KB</code>   – delete files of exactly 512 KB\n\n"
            "<i>Units: KB, MB, GB, TB (case-insensitive)</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    raw = message.text.split(None, 1)[1].strip()
    parsed = parse_size_string(raw)
    if not parsed:
        await message.reply_text(
            f"❌ <b>Cannot parse:</b> <code>{raw}</code>\n\n"
            "Use formats like: <code>&lt;100MB</code>, <code>&gt;=1GB</code>, "
            "<code>=512KB</code>, <code>0.5GB</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    try:
        count, label = await count_indexed_files_by_size(raw)
    except Exception as e:
        logger.exception(e)
        await message.reply_text(f"<b>❌ Error:</b> <code>{e}</code>", parse_mode=enums.ParseMode.HTML)
        return

    if count == 0:
        await message.reply_text(
            f"✅ <b>No indexed files found matching:</b> <code>{label}</code>\n\n"
            "<i>Nothing to delete.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    await message.reply_text(
        f"⚠️ <b>Found <u>{count} indexed file(s)</u> matching:</b> <code>{label}</code>\n\n"
        f"Do you want to <b>permanently delete these {count} file(s)</b> from the database?",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=_confirm_btns(_enc(raw)),
    )


# ──────────────────────────────────────────────────────────────────
# Callbacks — Yes / No for size-based deletion
# ──────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^szdel#") & filters.user(ADMINS))
async def cb_size_delete(client: Client, query: CallbackQuery):
    await query.answer()
    expr_enc = query.data.split("#", 1)[1]
    try:
        raw = _dec(expr_enc)
    except Exception:
        await query.message.edit_text(
            "<b>❌ Invalid data. Please retry.</b>", parse_mode=enums.ParseMode.HTML
        )
        return

    await query.message.edit_text(
        f"⏳ <b>Deleting files matching:</b> <code>{raw}</code>…",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        deleted, label = await delete_indexed_files_by_size(raw)
    except Exception as e:
        logger.exception(e)
        await query.message.edit_text(
            f"<b>❌ Error during deletion:</b>\n<code>{e}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    await query.message.edit_text(
        f"🗑️ <b>Done! Deleted <code>{deleted}</code> file(s)</b>\n"
        f"<b>Threshold:</b> below <code>{get_size(bval)}</code>\n\n"
        "<i>These files will not appear in search results unless re-indexed.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^szkeep$") & filters.user(ADMINS))
async def cb_size_keep(client: Client, query: CallbackQuery):
    await query.answer("Cancelled — no files were deleted.", show_alert=False)
    await query.message.edit_text(
        "❌ <b>Cancelled.</b> No files were deleted.",
        parse_mode=enums.ParseMode.HTML,
    )
