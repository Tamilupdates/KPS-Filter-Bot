"""
spam_warnings_db.py
───────────────────
MongoDB-backed storage for spam warnings per user per chat.

Collection schema (one document per user+chat pair):
  {
    _id:       ObjectId,
    chat_id:   int,
    user_id:   int,
    count:     int,          # 1, 2, or 3 (banned on 4th violation)
    reasons:   [str],        # list of all violation reason strings
    updated_at: datetime     # last warning timestamp (IST-aware)
  }
"""

import logging
import pytz
from datetime import datetime
import motor.motor_asyncio
from info import OTHER_DB_URI, DATABASE_NAME

logger = logging.getLogger(__name__)

_IST = pytz.timezone("Asia/Kolkata")

_client = motor.motor_asyncio.AsyncIOMotorClient(OTHER_DB_URI)
_db     = _client[DATABASE_NAME]
_col    = _db["spam_warnings"]


def _now_ist() -> datetime:
    return datetime.now(_IST)


# ──────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────

async def get_warning_count(chat_id: int, user_id: int) -> int:
    """Return the current warning count for a user in a chat (0 if none)."""
    doc = await _col.find_one({"chat_id": chat_id, "user_id": user_id})
    return doc["count"] if doc else 0


async def add_warning(chat_id: int, user_id: int, reason: str) -> int:
    """
    Increment the warning counter by 1 and append the reason.
    Returns the NEW total count after incrementing.
    """
    doc = await _col.find_one({"chat_id": chat_id, "user_id": user_id})
    if doc:
        new_count = doc["count"] + 1
        await _col.update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {
                "$set":  {"count": new_count, "updated_at": _now_ist()},
                "$push": {"reasons": reason},
            },
        )
    else:
        new_count = 1
        await _col.insert_one(
            {
                "chat_id":    chat_id,
                "user_id":    user_id,
                "count":      new_count,
                "reasons":    [reason],
                "updated_at": _now_ist(),
            }
        )
    return new_count


async def reset_warnings(chat_id: int, user_id: int) -> None:
    """Delete the warning record for a user in a chat (called after ban)."""
    await _col.delete_one({"chat_id": chat_id, "user_id": user_id})


async def get_warning_doc(chat_id: int, user_id: int) -> dict | None:
    """Return the full warning document, or None."""
    return await _col.find_one({"chat_id": chat_id, "user_id": user_id})


async def get_all_warned_users(chat_id: int) -> list:
    """Return all warning documents for a given chat."""
    return await _col.find({"chat_id": chat_id}).to_list(length=None)
