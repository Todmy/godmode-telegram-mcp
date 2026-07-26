"""SQLite cache layer — async read/write with TTL per data type.

TTLs:
    channels:    3600s  (1 hour)
    messages:     900s  (15 minutes)
    folders:     3600s  (1 hour)
    subscribers: 21600s (6 hours)

Cache key = operation name + serialized params.
Write operations invalidate related cache entries.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import aiosqlite

from tg_mcp.config import logger


class CacheCategory(Enum):
    """Cache categories with their TTL in seconds."""

    CHANNELS = 3600
    MESSAGES = 900
    FOLDERS = 3600
    SUBSCRIBERS = 21600


_CATEGORY_MAP: dict[str, CacheCategory] = {
    "channels": CacheCategory.CHANNELS,
    "messages": CacheCategory.MESSAGES,
    "folders": CacheCategory.FOLDERS,
    "subscribers": CacheCategory.SUBSCRIBERS,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(iso_str: str) -> datetime:
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def make_cache_key(operation: str, params: dict[str, Any] | None = None) -> str:
    """Build a deterministic cache key from operation name + params."""
    if not params:
        return operation

    parts = []
    for k in sorted(params.keys()):
        v = params[k]
        if v is None:
            continue
        parts.append(f"{k}={v}")

    if not parts:
        return operation

    return f"{operation}:{','.join(parts)}"


def resolve_category(name: str) -> CacheCategory:
    """Resolve a category name to its enum. Raises ValueError if unknown."""
    cat = _CATEGORY_MAP.get(name.lower())
    if cat is None:
        raise ValueError(
            f"Unknown cache category: {name!r}. "
            f"Valid categories: {', '.join(_CATEGORY_MAP.keys())}"
        )
    return cat


async def invalidate_keys(cache: Cache | None, *keys: str) -> None:
    """Best-effort cache invalidation for write operations.

    No-op when the cache or the shared db connection is unavailable, so write
    operations never fail because of cache bookkeeping.
    """
    if cache is None or not keys:
        return

    from tg_mcp.db import current_db

    db = current_db()
    if db is None:
        return

    for key in keys:
        await cache.invalidate(db, key)


class Cache:
    """Async SQLite cache with TTL checking.

    All methods accept an open aiosqlite connection.
    The cache does NOT own the connection — caller manages lifecycle.
    """

    async def is_fresh(
        self,
        db: aiosqlite.Connection,
        key: str,
        category: CacheCategory,
    ) -> bool:
        """Check if a cache entry exists and is still within its TTL."""
        try:
            cursor = await db.execute(
                "SELECT cached_at, ttl_seconds FROM cache_meta WHERE key = ?",
                (key,),
            )
            row = await cursor.fetchone()
        except aiosqlite.Error:
            logger.exception("cache.is_fresh_error", extra={"key": key})
            return False

        if row is None:
            return False

        cached_at_str, ttl_seconds = row

        try:
            cached_at = _parse_iso(cached_at_str)
        except (ValueError, TypeError):
            logger.warning("cache.corrupted_timestamp", extra={"key": key, "value": cached_at_str})
            return False

        now = datetime.now(timezone.utc)
        age_seconds = (now - cached_at).total_seconds()

        # Cap at category TTL to prevent stale values from old code versions
        effective_ttl = min(ttl_seconds, category.value)

        return age_seconds < effective_ttl

    async def mark_fresh(
        self,
        db: aiosqlite.Connection,
        key: str,
        category: CacheCategory,
    ) -> None:
        """Record that data for this key was just cached."""
        now = _now_iso()
        try:
            await db.execute(
                """INSERT INTO cache_meta (key, cached_at, ttl_seconds)
                   VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET
                     cached_at = excluded.cached_at,
                     ttl_seconds = excluded.ttl_seconds""",
                (key, now, category.value),
            )
            await db.commit()
        except aiosqlite.Error:
            logger.exception("cache.mark_fresh_error", extra={"key": key})

    async def invalidate(
        self,
        db: aiosqlite.Connection,
        pattern: str,
    ) -> int:
        """Invalidate cache entries matching a key prefix."""
        try:
            cursor = await db.execute(
                "DELETE FROM cache_meta WHERE key = ? OR key LIKE ?",
                (pattern, f"{pattern}:%"),
            )
            count = cursor.rowcount
            await db.commit()
            if count > 0:
                logger.info(
                    "cache.invalidated",
                    extra={"pattern": pattern, "count": count},
                )
            return count
        except aiosqlite.Error:
            logger.exception("cache.invalidate_error", extra={"pattern": pattern})
            return 0

    async def invalidate_all(self, db: aiosqlite.Connection) -> int:
        """Invalidate all cache entries."""
        try:
            cursor = await db.execute("DELETE FROM cache_meta")
            count = cursor.rowcount
            await db.commit()
            logger.info("cache.invalidated_all", extra={"count": count})
            return count
        except aiosqlite.Error:
            logger.exception("cache.invalidate_all_error")
            return 0

    async def get_channels(
        self,
        db: aiosqlite.Connection,
    ) -> list[dict[str, Any]] | None:
        """Read cached channel list. Returns None if not cached or expired."""
        key = make_cache_key("channels")
        if not await self.is_fresh(db, key, CacheCategory.CHANNELS):
            return None

        try:
            cursor = await db.execute(
                """SELECT id, title, handle, subscribers, is_channel, folder,
                          last_post_date, posts_per_week, unread_count, cached_at
                   FROM channels"""
            )
            rows = await cursor.fetchall()
        except aiosqlite.Error:
            logger.exception("cache.get_channels_error")
            return None

        if not rows:
            return None

        columns = [
            "id", "title", "handle", "subscribers", "is_channel", "folder",
            "last_post_date", "posts_per_week", "unread_count", "cached_at",
        ]
        return [dict(zip(columns, row)) for row in rows]

    async def put_channels(
        self,
        db: aiosqlite.Connection,
        channels: list[dict[str, Any]],
    ) -> None:
        """Cache a list of channels. Replaces existing data."""
        now = _now_iso()
        try:
            await db.execute("DELETE FROM channels")

            for ch in channels:
                await db.execute(
                    """INSERT INTO channels
                       (id, title, handle, subscribers, is_channel, folder,
                        last_post_date, posts_per_week, unread_count, cached_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ch["id"], ch["title"], ch.get("handle"),
                        ch.get("subscribers"), ch.get("is_channel", True),
                        ch.get("folder"), ch.get("last_post_date"),
                        ch.get("posts_per_week"), ch.get("unread_count", 0),
                        now,
                    ),
                )

            await db.commit()
            await self.mark_fresh(db, make_cache_key("channels"), CacheCategory.CHANNELS)
        except aiosqlite.Error:
            logger.exception("cache.put_channels_error")

    async def get_messages(
        self,
        db: aiosqlite.Connection,
        channel_id: int,
        limit: int = 20,
    ) -> list[dict[str, Any]] | None:
        """Read cached messages for a channel. Returns None if expired."""
        key = make_cache_key("messages", {"channel_id": channel_id})
        if not await self.is_fresh(db, key, CacheCategory.MESSAGES):
            return None

        try:
            cursor = await db.execute(
                """SELECT id, channel_id, date, text, author, views,
                          reactions_json, replies, forward_from, media_type,
                          media_filename, media_file_size, media_download_link,
                          cached_at
                   FROM messages
                   WHERE channel_id = ?
                   ORDER BY date DESC
                   LIMIT ?""",
                (channel_id, limit),
            )
            rows = await cursor.fetchall()
        except aiosqlite.Error:
            logger.exception("cache.get_messages_error", extra={"channel_id": channel_id})
            return None

        if not rows:
            return None

        columns = [
            "id", "channel_id", "date", "text", "author", "views",
            "reactions_json", "replies", "forward_from", "media_type",
            "media_filename", "media_file_size", "media_download_link",
            "cached_at",
        ]
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            if d.get("reactions_json"):
                try:
                    d["reactions"] = json.loads(d["reactions_json"])
                except (json.JSONDecodeError, TypeError):
                    d["reactions"] = {}
            else:
                d["reactions"] = {}
            results.append(d)

        return results

    async def put_messages(
        self,
        db: aiosqlite.Connection,
        channel_id: int,
        messages: list[dict[str, Any]],
    ) -> None:
        """Cache messages for a channel. Uses upsert."""
        now = _now_iso()
        try:
            for msg in messages:
                reactions_json = None
                if msg.get("reactions"):
                    reactions_json = json.dumps(msg["reactions"], ensure_ascii=False)

                await db.execute(
                    """INSERT INTO messages
                       (id, channel_id, date, text, author, views,
                        reactions_json, replies, forward_from, media_type,
                        media_filename, media_file_size, media_download_link,
                        cached_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(channel_id, id) DO UPDATE SET
                         text = excluded.text,
                         views = excluded.views,
                         reactions_json = excluded.reactions_json,
                         replies = excluded.replies,
                         cached_at = excluded.cached_at""",
                    (
                        msg["id"], channel_id, msg["date"],
                        msg.get("text"), msg.get("author"),
                        msg.get("views", 0), reactions_json,
                        msg.get("replies", 0), msg.get("forward_from"),
                        msg.get("media_type"),
                        msg.get("media_filename"),
                        msg.get("media_file_size"),
                        msg.get("media_download_link"),
                        now,
                    ),
                )

            await db.commit()
            key = make_cache_key("messages", {"channel_id": channel_id})
            await self.mark_fresh(db, key, CacheCategory.MESSAGES)
        except aiosqlite.Error:
            logger.exception("cache.put_messages_error", extra={"channel_id": channel_id})

    async def get_folders(
        self,
        db: aiosqlite.Connection,
    ) -> list[dict[str, Any]] | None:
        """Read cached folder list. Returns None if not cached or expired."""
        key = make_cache_key("folders")
        if not await self.is_fresh(db, key, CacheCategory.FOLDERS):
            return None

        try:
            cursor = await db.execute(
                "SELECT id, title, channel_ids_json, cached_at FROM folders"
            )
            rows = await cursor.fetchall()
        except aiosqlite.Error:
            logger.exception("cache.get_folders_error")
            return None

        if not rows:
            return None

        results = []
        for row in rows:
            d = {
                "id": row[0],
                "title": row[1],
                "channel_ids": [],
                "cached_at": row[3],
            }
            if row[2]:
                try:
                    d["channel_ids"] = json.loads(row[2])
                except (json.JSONDecodeError, TypeError):
                    d["channel_ids"] = []
            results.append(d)

        return results

    async def put_folders(
        self,
        db: aiosqlite.Connection,
        folders: list[dict[str, Any]],
    ) -> None:
        """Cache folder list. Replaces existing data."""
        now = _now_iso()
        try:
            await db.execute("DELETE FROM folders")

            for f in folders:
                channel_ids_json = None
                if f.get("channel_ids"):
                    channel_ids_json = json.dumps(f["channel_ids"])

                await db.execute(
                    """INSERT INTO folders (id, title, channel_ids_json, cached_at)
                       VALUES (?, ?, ?, ?)""",
                    (f["id"], f["title"], channel_ids_json, now),
                )

            await db.commit()
            await self.mark_fresh(db, make_cache_key("folders"), CacheCategory.FOLDERS)
        except aiosqlite.Error:
            logger.exception("cache.put_folders_error")
