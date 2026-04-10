# Godmode Telegram MCP

MCP server exposing full Telegram client capabilities to Claude Code. Uses Telethon (User API / MTProto) for channel reading, message search, reactions, folder management, and analytics across 200+ subscribed channels.

## Architecture

5 static MCP tools + dynamic operations catalog (23+ ops). Speakeasy Dynamic Toolsets pattern: `tg_search_ops` → `tg_describe_op` → `tg_execute`. Two shortcut tools for high-frequency ops: `tg_feed` (messages) and `tg_overview` (channels).

Full spec: `docs/spec.md`

## Stack

- Python 3.11+, async throughout
- `mcp` SDK (pip) — MCP server, stdio transport
- `telethon` — Telegram User API (MTProto)
- `aiosqlite` — async SQLite cache (WAL mode)
- `python-dotenv` — config from `.env`

## Project Structure

```
src/tg_mcp/
  server.py      — MCP entry point (5 tools)
  client.py      — Telethon wrapper (lazy connect)
  cache.py       — SQLite cache (TTL-based)
  catalog.py     — Operations registry + search
  toon.py        — TOON format serializer
  config.py      — Settings loader
  auth.py        — One-time Telegram auth CLI
  ops/           — Operation modules (channels, messages, interact, folders, analytics)
  db/            — SQLite schema + migrations
```

## Commands

```bash
# Run MCP server (called by Claude Code automatically)
python -m tg_mcp

# Authenticate with Telegram (one-time)
python -m tg_mcp.auth

# Run tests
pytest tests/
```

## Code Style

- Async/await everywhere (Telethon is async)
- Type hints on all functions
- Operations registered via `@operation()` decorator in `catalog.py`
- Responses use TOON format for lists (see `toon.py`)
- Errors must include: what happened + expected + example + recovery hint
- No print() — use MCP logging or structured logs

## Key Design Decisions

1. **5 static tools max** — everything else through dynamic toolsets (token efficiency)
2. **TOON format** for list responses — 30-60% token reduction vs JSON
3. **Lazy Telethon connection** — connect on first tool call, not on MCP startup
4. **SQLite cache with TTL** — avoid redundant Telegram API calls within session
5. **Destructive ops require confirm=true** — unsubscribe, delete, etc.
6. **Outcome-oriented operations** — `who_posted_first`, `find_duplicates`, not raw API wrappers

## MCP Design Patterns Applied

All patterns from `research/mcp-design-patterns-guide.md`:
- Dynamic Toolsets (Speakeasy, §8.2) — 96.7% token reduction
- TOON response format (§2.4)
- Minimal default fields + `fields` param (§6.2)
- Content truncation with hints (§6.3)
- Pre-computed aggregates (§6.4)
- Definitive empty states (§6.5)
- Next-step hints (§6.6)
- Three-tier error handling with isError (§7)
- No anti-patterns from §9

## Data Locations

- Session + cache + config: `~/.tg-mcp/`
- Source code: `/Users/todmy/github/telegram-mcp/`
- Never commit `.env` or `session.session`

## Active Technologies
- Python 3.11+ (async generators, ExceptionGroup support) + mcp SDK (pip), telethon >=1.36, aiosqlite >=0.20, python-dotenv >=1.0, cryptg >=0.4 (001-telegram-mcp-server)
- SQLite with WAL mode (aiosqlite) — channel index, message cache, folder structure (001-telegram-mcp-server)

## Recent Changes
- 001-telegram-mcp-server: Added Python 3.11+ (async generators, ExceptionGroup support) + mcp SDK (pip), telethon >=1.36, aiosqlite >=0.20, python-dotenv >=1.0, cryptg >=0.4
