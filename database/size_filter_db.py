"""
size_filter_db.py
─────────────────
MongoDB-backed store for file-size filters.

Each document represents one size rule:
  {
    _id:        ObjectId,
    label:      str,        # human-readable, e.g. "100 MB"
    bytes:      int,        # parsed byte value
    operator:   str,        # "lt" | "lte" | "gt" | "gte" | "eq"
    created_at: datetime
  }

During indexing, a file is skipped if its file_size matches ANY active rule.

Supported input formats (case-insensitive):
  100MB  100 MB  0.5GB  512KB  1.2 GB  <100MB  <=512KB  >1GB  >=2GB  =100MB
"""

import re
import logging
import motor.motor_asyncio
from datetime import datetime
import pytz

from info import OTHER_DB_URI, DATABASE_NAME

logger = logging.getLogger(__name__)

_IST = pytz.timezone("Asia/Kolkata")
# ── Size-filter RULES stored in OTHER_DB_URI / DATABASE_NAME / "size_filters" ──
# (When MULTIPLE_DATABASE=False, OTHER_DB_URI == FILE_DB_URI == DATABASE_URI)
_client = motor.motor_asyncio.AsyncIOMotorClient(OTHER_DB_URI)
_db = _client[DATABASE_NAME]
_col = _db["size_filters"]   # dedicated collection for filter rules (not file data)

# Cache loaded at startup / refreshed on mutations
_SIZE_RULES: list = []   # list of {"bytes": int, "operator": str, "label": str}

# ──────────────────────────────────────────────────────────
# Parsing helpers
# ──────────────────────────────────────────────────────────

_UNIT_MAP = {
    "b":   1,
    "kb":  1024,
    "mb":  1024 ** 2,
    "gb":  1024 ** 3,
    "tb":  1024 ** 4,
}

_OP_MAP = {
    "<=": "lte",
    ">=": "gte",
    "<":  "lt",
    ">":  "gt",
    "=":  "eq",
    "":   "eq",   # bare number → exact match
}

_PARSE_RE = re.compile(
    r"^(?P<op>[<>]=?|=)?\s*(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>[kmgt]?b)$",
    re.IGNORECASE,
)


def parse_size_string(raw: str) -> tuple[int, str, str] | None:
    """
    Parse a size string like "100MB", "<500KB", ">=1GB".
    Returns (bytes_value, operator_code, canonical_label) or None on failure.
    """
    raw = raw.strip()
    m = _PARSE_RE.match(raw)
    if not m:
        return None
    op_sym  = m.group("op") or ""
    num     = float(m.group("num"))
    unit    = m.group("unit").lower()
    factor  = _UNIT_MAP.get(unit, 1)
    bval    = int(num * factor)
    op_code = _OP_MAP[op_sym]
    # canonical label  e.g. "100 MB" or "<= 512 KB"
    op_display = "" if op_sym == "" else (op_sym + " ")
    label = f"{op_display}{num:g} {unit.upper()}"
    return bval, op_code, label


def matches_size_rule(file_size_bytes: int) -> bool:
    """Return True if file_size matches any active size filter rule."""
    for rule in _SIZE_RULES:
        bval = rule["bytes"]
        op   = rule["operator"]
        if op == "eq"  and file_size_bytes == bval: return True
        if op == "lt"  and file_size_bytes <  bval: return True
        if op == "lte" and file_size_bytes <= bval: return True
        if op == "gt"  and file_size_bytes >  bval: return True
        if op == "gte" and file_size_bytes >= bval: return True
    return False


# ──────────────────────────────────────────────────────────
# Cache refresh
# ──────────────────────────────────────────────────────────

async def _refresh_cache():
    global _SIZE_RULES
    docs = await _col.find({}).to_list(length=None)
    _SIZE_RULES = [{"bytes": d["bytes"], "operator": d["operator"], "label": d["label"]} for d in docs]
    logger.info(f"[size_filter] cache refreshed – {len(_SIZE_RULES)} rule(s)")


# ──────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────

async def load_size_filters():
    """Call once at startup to warm the in-memory cache."""
    await _refresh_cache()


async def get_all_size_rules() -> list[dict]:
    """Return all rules with id, label, bytes, operator."""
    docs = await _col.find({}).to_list(length=None)
    return [{"id": str(d["_id"]), "label": d["label"],
             "bytes": d["bytes"], "operator": d["operator"]} for d in docs]


async def add_size_rule(raw: str) -> tuple[bool, str]:
    """
    Parse and add a size rule.
    Returns (True, label) on success, (False, error_msg) on failure.
    """
    parsed = parse_size_string(raw)
    if not parsed:
        return False, (
            f"❌ Cannot parse <code>{raw}</code>.\n\n"
            "Use formats like: <code>100MB</code>, <code>&lt;500KB</code>, "
            "<code>&gt;=1GB</code>, <code>0.5GB</code>"
        )
    bval, op_code, label = parsed
    # duplicate check
    existing = await _col.find_one({"bytes": bval, "operator": op_code})
    if existing:
        return False, f"⚠️ Rule <code>{label}</code> already exists."
    await _col.insert_one({
        "label": label, "bytes": bval, "operator": op_code,
        "created_at": datetime.now(_IST),
    })
    await _refresh_cache()
    return True, label


async def remove_size_rule(raw: str) -> tuple[bool, str]:
    """
    Remove a size rule by parsing the same size string.
    Returns (True, label) or (False, error_msg).
    """
    parsed = parse_size_string(raw)
    if not parsed:
        return False, f"❌ Cannot parse <code>{raw}</code>. Use /listsize to see active rules."
    bval, op_code, label = parsed
    result = await _col.delete_one({"bytes": bval, "operator": op_code})
    if result.deleted_count:
        await _refresh_cache()
        return True, label
    return False, f"❌ Rule <code>{label}</code> not found. Use /listsize."


# ──────────────────────────────────────────────────────────
# Indexed-file helpers
# ──────────────────────────────────────────────────────────

def _build_size_mongo_query(bval: int, op_code: str) -> dict:
    mongo_ops = {"lt": "$lt", "lte": "$lte", "gt": "$gt", "gte": "$gte", "eq": None}
    mop = mongo_ops[op_code]
    if mop:
        return {"file_size": {mop: bval}}
    return {"file_size": bval}


async def count_indexed_files_by_size(raw: str):
    """
    Count indexed files matching a size expression.
    Returns (count, label) on success, raises ValueError on parse failure.
    Uses Media.collection (same connection the rest of the bot uses).
    """
    from database.ia_filterdb import Media  # deferred to avoid circular import
    parsed = parse_size_string(raw)
    if not parsed:
        raise ValueError(f"Cannot parse size expression: {raw!r}")
    bval, op_code, label = parsed
    count = await Media.collection.count_documents(_build_size_mongo_query(bval, op_code))
    return count, label


async def count_and_size_indexed_files(raw: str):
    """
    Return (file_count, total_bytes) for all files matching the size expression.
    Uses a single MongoDB aggregation pipeline — zero extra round-trips.
    """
    from database.ia_filterdb import Media  # deferred to avoid circular import
    parsed = parse_size_string(raw)
    if not parsed:
        raise ValueError(f"Cannot parse size expression: {raw!r}")
    bval, op_code, _ = parsed
    match = {"$match": _build_size_mongo_query(bval, op_code)}
    group = {"$group": {"_id": None, "count": {"$sum": 1}, "total": {"$sum": "$file_size"}}}
    pipeline = [match, group]
    result = await Media.collection.aggregate(pipeline).to_list(length=1)
    if not result:
        return 0, 0
    return result[0]["count"], result[0]["total"]


async def delete_indexed_files_by_size(raw: str):
    """
    Delete all indexed files matching a size expression.
    Returns (deleted_count, label) on success, raises ValueError on parse failure.
    Uses Media.collection (same connection the rest of the bot uses).
    """
    from database.ia_filterdb import Media  # deferred to avoid circular import
    parsed = parse_size_string(raw)
    if not parsed:
        raise ValueError(f"Cannot parse size expression: {raw!r}")
    bval, op_code, label = parsed
    result = await Media.collection.delete_many(_build_size_mongo_query(bval, op_code))
    return result.deleted_count, label
