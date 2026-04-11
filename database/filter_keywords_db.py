"""
filter_keywords_db.py
─────────────────────
MongoDB-backed store for blocked keywords.

Collection schema  (one document per keyword):
  { _id: <ObjectId>, keyword: str (lowercase, stripped) }

The bot-wide cache (`temp.FILTER_KEYWORDS`) is updated by every
mutating operation so the running bot sees changes immediately without
a restart.
"""

import logging
import re
import motor.motor_asyncio
from info import OTHER_DB_URI, DATABASE_NAME

logger = logging.getLogger(__name__)

_client = motor.motor_asyncio.AsyncIOMotorClient(OTHER_DB_URI)
_db = _client[DATABASE_NAME]
_col = _db["filter_keywords"]


# ──────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────

def _norm(keyword: str) -> str:
    """Normalise a keyword: strip whitespace, lowercase."""
    return keyword.strip().lower()


async def _refresh_cache():
    """Re-read all keywords from Mongo and push into temp.FILTER_KEYWORDS."""
    from utils import temp  # deferred import to avoid circular dependency
    docs = await _col.find({}, {"keyword": 1}).to_list(length=None)
    temp.FILTER_KEYWORDS = [doc["keyword"] for doc in docs]
    logger.info(f"[filter_keywords] cache refreshed – {len(temp.FILTER_KEYWORDS)} keyword(s)")


# ──────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────

async def load_keywords_to_cache():
    """
    Called once at bot startup.
    Merges env-var defaults that are NOT already in Mongo, then warms the
    in-memory cache.
    """
    from info import FILTER_KEYWORDS as ENV_KEYWORDS  # static list from info.py

    existing = {doc["keyword"] async for doc in _col.find({}, {"keyword": 1})}
    new_docs = [
        {"keyword": kw}
        for kw in ENV_KEYWORDS
        if kw not in existing
    ]
    if new_docs:
        await _col.insert_many(new_docs)
        logger.info(f"[filter_keywords] seeded {len(new_docs)} keyword(s) from env-var")

    await _refresh_cache()


async def get_all_keywords() -> list:
    """Return a sorted list of all blocked keywords."""
    docs = await _col.find({}, {"keyword": 1}).to_list(length=None)
    return sorted(doc["keyword"] for doc in docs)


async def add_keyword(keyword: str) -> bool:
    """
    Add a new keyword.
    Returns True if inserted, False if it already existed.
    """
    kw = _norm(keyword)
    if not kw:
        return False
    existing = await _col.find_one({"keyword": kw})
    if existing:
        return False
    await _col.insert_one({"keyword": kw})
    await _refresh_cache()
    return True


async def remove_keyword(keyword: str) -> bool:
    """
    Remove a keyword.
    Returns True if deleted, False if it was not found.
    """
    kw = _norm(keyword)
    result = await _col.delete_one({"keyword": kw})
    if result.deleted_count:
        await _refresh_cache()
        return True
    return False


async def keyword_exists(keyword: str) -> bool:
    """Check whether a keyword is already in the list."""
    kw = _norm(keyword)
    doc = await _col.find_one({"keyword": kw})
    return doc is not None


# ──────────────────────────────────────────────────────────
# Indexed-file helpers (operate on the Media/files collection)
# ──────────────────────────────────────────────────────────

async def count_files_with_keyword(keyword: str) -> int:
    """
    Count how many already-indexed files in the Media collection
    have the keyword in their file_name or caption (case-insensitive).
    """
    from info import FILE_DB_URI, COLLECTION_NAME
    kw = _norm(keyword)
    if not kw:
        return 0
    file_client = motor.motor_asyncio.AsyncIOMotorClient(FILE_DB_URI)
    file_db = file_client[DATABASE_NAME]
    col = file_db[COLLECTION_NAME]
    regex = re.compile(re.escape(kw), re.IGNORECASE)
    query = {"$or": [{"file_name": regex}, {"caption": regex}]}
    return await col.count_documents(query)


async def delete_files_with_keyword(keyword: str) -> int:
    """
    Delete all indexed files whose file_name or caption contains
    the keyword (case-insensitive).  Returns the number deleted.
    """
    from info import FILE_DB_URI, COLLECTION_NAME
    kw = _norm(keyword)
    if not kw:
        return 0
    file_client = motor.motor_asyncio.AsyncIOMotorClient(FILE_DB_URI)
    file_db = file_client[DATABASE_NAME]
    col = file_db[COLLECTION_NAME]
    regex = re.compile(re.escape(kw), re.IGNORECASE)
    query = {"$or": [{"file_name": regex}, {"caption": regex}]}
    result = await col.delete_many(query)
    return result.deleted_count
