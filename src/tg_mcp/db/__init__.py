"""Database initialization and connection management.

Provides get_db() — the single entry point for obtaining a database connection.
On first call, applies schema and sets WAL mode.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import aiosqlite

from tg_mcp.config import logger
from tg_mcp.db.migrations import apply_migrations

# Module-level connection: one per process, reused across calls.
_db: aiosqlite.Connection | None = None
_db_lock = asyncio.Lock()


async def get_db(db_path: Path) -> aiosqlite.Connection:
    """Get or create the shared database connection.

    Thread-safe via asyncio lock. Applies migrations on first connect.
    Sets WAL mode and foreign keys pragmas.
    """
    global _db

    async with _db_lock:
        if _db is not None:
            try:
                await _db.execute("SELECT 1")
                return _db
            except Exception:
                logger.warning("db.connection_lost", extra={"path": str(db_path)})
                _db = None

        logger.info("db.connecting", extra={"path": str(db_path)})

        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = await aiosqlite.connect(str(db_path))

        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute("PRAGMA busy_timeout=5000")

        await apply_migrations(conn)

        _db = conn
        logger.info("db.connected", extra={"path": str(db_path)})
        return conn


def current_db() -> aiosqlite.Connection | None:
    """Return the shared connection if one is already open, else None.

    Used by write operations that need to invalidate cache entries without
    knowing the configured db path.
    """
    return _db


async def close_db() -> None:
    """Close the shared database connection if open."""
    global _db

    async with _db_lock:
        if _db is not None:
            try:
                await _db.close()
                logger.info("db.closed")
            except Exception:
                logger.exception("db.close_error")
            finally:
                _db = None
