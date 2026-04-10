# Godmode Telegram MCP — Design Specification

**Created:** 2026-04-09
**Status:** Approved for implementation
**Location:** `/Users/todmy/github/telegram-mcp/`

---

## 1. Problem Statement

200+ Telegram channel subscriptions create information overload. No way to read, analyze, filter, group, or manage channels from the terminal. Existing Telegram plugin (Bot API) can only receive DMs — cannot read subscribed channels.

**Goal:** MCP server that exposes full Telegram client capabilities to Claude Code. On-demand queries (no background daemon). Designed for maximum token efficiency and extensibility.

**User:** Single-user (personal Telegram account), used exclusively from Claude Code terminal.

---

## 2. Architecture

```
Claude Code
    ↕ MCP Protocol (stdio)
MCP Server (Python, 5 static tools)
    ↕ function calls
Operations Catalog (~20 initial ops, grows over time)
    ↕ async calls
Telethon Client (MTProto User API)
    ↕ MTProto
Telegram Servers
```

### 2.1 Three Layers

| Layer | Responsibility | Technology |
|-------|---------------|------------|
| **MCP Layer** | 5 static tools, TOON responses, token efficiency, tool annotations | `mcp` Python SDK (pip) |
| **Operations Catalog** | Registry of all Telegram operations. Each = async Python function + JSON schema. Discoverable via `tg_search_ops`. New ops = new file in `ops/`, auto-registered | Python modules with decorator-based registration |
| **Telethon Core** | Async client, session persistence, connection pooling, FloodWait handling, rate limiting. Reusable module (future: Telegram Claude Bot project) | Telethon 1.x + aiosqlite |

### 2.2 Design Decisions

| Decision | Choice | Why |
|----------|--------|-----|
| API | Telethon (User API / MTProto) | Bot API cannot read subscribed channels. User API gives full client capabilities |
| Transport | stdio | Standard for Claude Code MCP servers. No HTTP overhead |
| Storage | SQLite (WAL mode) | Channel index + message cache. Single-file, no external deps. WAL for concurrent reads |
| Tool pattern | Dynamic Toolsets (Speakeasy §8.2) | 200+ potential operations, ≤5 tools in context. 96.7% token reduction. Constant cost regardless of catalog size |
| Response format | TOON for lists, plain text for single items | 30-60% token reduction on list responses (§2.4) |
| Session | File-based (`~/.tg-mcp/session.session`) | Telethon default. Persistent auth, no re-login per request |
| Config | `.env` file | API_ID, API_HASH, PHONE. Standard pattern, matches planned Telegram Claude Bot |

---

## 3. MCP Tools (5 Static)

Following §3.1 (tool count cliff) and §8.2 (dynamic toolsets). Five tools permanently loaded into Claude's context. Everything else accessed through the discover → describe → execute pattern.

### 3.1 `tg_feed` — Read channel messages

**Rationale:** Highest-frequency operation (~60% of all requests). Direct shortcut avoids 3-step dance.

```
Purpose: Fetch recent messages from one or more Telegram channels/groups.
Returns messages with author, date, text, views, reactions, reply count.
For single-channel deep dive, use channel handle. For multi-channel digest, omit channel to get cross-channel feed sorted by time.
Truncates message text at 300 chars by default — use include_full_text=true for complete content.
For searching by keyword across channels, use tg_search_ops to find the search operation instead.
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `channel` | string | no | all subscribed | Channel @handle, t.me link, or channel title substring. Omit for cross-channel feed |
| `limit` | int | no | 20 | Messages to return (1-100) |
| `hours` | int | no | 24 | Time window in hours (1-720) |
| `fields` | string[] | no | [text, date, views] | Fields: text, date, author, views, reactions, replies, forward_from, media_type |
| `include_full_text` | bool | no | false | Return full message text without truncation |
| `folder` | string | no | none | Telegram folder name to filter channels |

**Response format:** TOON (§2.4)

```
feed[15]{date,channel,text,views,reactions}:
2026-04-09T14:32,@llm_under_hood,RAG pipeline anti-patterns I've seen in prod this month...(truncated 1240 chars → include_full_text=true),4521,89
2026-04-09T13:15,@techsparks,Anthropic just published their economic impact study...(truncated 890 chars → include_full_text=true),7832,156
...

summary: 15 messages from 8 channels | 24h window | avg 3.2K views
→ For full message: call tg_feed with channel=@handle and include_full_text=true
→ To search by keyword: tg_search_ops query="search messages"
→ To see channel stats: tg_search_ops query="channel statistics"
```

**Annotations:** `readOnlyHint: true`, `idempotentHint: true`

### 3.2 `tg_overview` — Channel/folder overview

**Rationale:** Second most frequent operation (~25%). Orientation tool — "what do I have, what's active, what's dead."

```
Purpose: Overview of subscribed channels, groups, and folders with activity metrics.
Returns channel list with subscriber count, post frequency, your unread count, and last post date.
Default sort: by unread count (most unread first). Use sort parameter to change.
For detailed stats on a single channel, use tg_search_ops to find the channel_stats operation.
For managing folders (create, move channels), use tg_search_ops query="folders".
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `sort` | enum | no | unread | Sort: unread, activity, subscribers, name, last_post |
| `folder` | string | no | all | Filter by Telegram folder name |
| `min_subscribers` | int | no | 0 | Minimum subscriber count filter |
| `type` | enum | no | all | Filter: channels, groups, all |
| `limit` | int | no | 50 | Channels to return (1-500) |
| `fields` | string[] | no | [name, unread, last_post] | Fields: name, handle, subscribers, unread, last_post, posts_per_week, folder, description |

**Response format:** TOON

```
channels[214]{name,handle,unread,last_post,posts_per_week,subscribers}:
LLM Under the Hood,@llm_under_hood,12,2026-04-09T10:00,4.2,25000
TechSparks,@techsparks,8,2026-04-09T09:30,5.1,33000
...

summary: 214 channels | 47 in folders | 3,284 total unread | 23 channels inactive >30d
→ To read messages: tg_feed channel=@handle
→ To see folders: tg_overview folder="AI News"
→ To manage subscriptions: tg_search_ops query="unsubscribe"
→ To manage folders: tg_search_ops query="folders"
```

**Annotations:** `readOnlyHint: true`, `idempotentHint: true`

### 3.3 `tg_search_ops` — Discover operations

**Rationale:** Dynamic toolset entry point (§8.2). Provides access to the full operations catalog without loading all schemas.

```
Purpose: Search the operations catalog by keyword or category.
Returns matching operation names with one-line descriptions.
Use this to discover what Telegram operations are available before calling tg_describe_op or tg_execute.
Categories: channels, messages, interact, folders, analytics, contacts, media.
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `query` | string | yes | — | Keyword search: "react", "unsubscribe", "forward", "folder", "history" |
| `category` | enum | no | all | Filter: channels, messages, interact, folders, analytics, contacts, media |

**Response format:** Plain text, compact

```
ops[5] matching "react":
  react_to_message — Add emoji reaction to a message in any channel/group
  get_reactions — Get reaction breakdown for a specific message
  top_reacted — Find most-reacted messages across channels in time window
  remove_reaction — Remove your reaction from a message
  reaction_analytics — Analyze reaction patterns across channels

→ To see full schema: tg_describe_op name="react_to_message"
→ To execute: tg_execute op="react_to_message" params={...}
```

**Annotations:** `readOnlyHint: true`, `idempotentHint: true`

### 3.4 `tg_describe_op` — Get operation schema

**Rationale:** Second step of dynamic toolset pattern. Loads full parameter schema only when needed.

```
Purpose: Get the full schema (parameters, types, defaults, description) for a specific operation.
Call tg_search_ops first to find the operation name, then this to see how to use it.
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | string | yes | — | Operation name from tg_search_ops results |

**Response format:** Structured text

```
op: react_to_message
description: Add emoji reaction to a message in any channel/group. Supports custom emoji.
category: interact
destructive: false
idempotent: true

params:
  channel (string, required) — Channel @handle or title
  message_id (int, required) — Message ID (from tg_feed or tg_execute search results)
  emoji (string, required) — Reaction emoji: 👍 ❤️ 🔥 😢 🤔 etc.

returns: confirmation with updated reaction count

example: tg_execute op="react_to_message" params={"channel": "@llm_under_hood", "message_id": 4521, "emoji": "🔥"}
```

**Annotations:** `readOnlyHint: true`, `idempotentHint: true`

### 3.5 `tg_execute` — Run any operation

**Rationale:** Universal executor. Third step of dynamic toolset. Handles both read and write operations.

```
Purpose: Execute any operation from the catalog by name with parameters.
Always call tg_describe_op first to see required parameters.
For destructive operations (unsubscribe, delete), returns confirmation prompt — pass confirm=true to proceed.
Rate-limited: Telegram enforces FloodWait — if hit, returns wait time.
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `op` | string | yes | — | Operation name |
| `params` | object | no | {} | Operation parameters as key-value pairs |
| `confirm` | bool | no | false | Required for destructive operations |
| `response_format` | enum | no | concise | concise (TOON/compact) or detailed (full data) |

**Response format:** Varies by operation (TOON for lists, plain text for single results)

**Annotations:** `readOnlyHint: false`, `idempotentHint: false` (varies by operation, but tool-level annotation must be conservative)

---

## 4. Operations Catalog (Day 1)

Operations organized by domain. Each operation = async Python function with decorator-based registration.

### 4.1 Registration Pattern

```python
from catalog import operation

@operation(
    name="channel_stats",
    category="analytics",
    description="Detailed statistics for a single channel: subscriber count, growth, post frequency, avg views, top posts, engagement rate",
    destructive=False,
    idempotent=True,
)
async def channel_stats(client, channel: str, days: int = 30, fields: list[str] | None = None):
    """Implementation here."""
    ...
```

The decorator registers the function + its type hints into the catalog. `tg_search_ops` searches names + descriptions. `tg_describe_op` introspects type hints + defaults to generate schema. `tg_execute` calls the function.

### 4.2 Day 1 Operations

#### Category: channels
| Operation | Description | Destructive |
|-----------|-------------|-------------|
| `list_channels` | List all subscribed channels with basic info | no |
| `channel_info` | Detailed info for a single channel (description, admins, creation date) | no |
| `channel_stats` | Activity stats: post frequency, avg views, engagement rate, growth | no |
| `subscribe` | Subscribe to a channel by handle/link | no |
| `unsubscribe` | Leave a channel. Requires confirm=true | **yes** |
| `mute_channel` | Mute/unmute notifications for a channel | no |

#### Category: messages
| Operation | Description | Destructive |
|-----------|-------------|-------------|
| `search_messages` | Search messages by keyword across channels with date filters | no |
| `get_message` | Get a single message by ID with full content and media info | no |
| `message_history` | Download message history for a channel (paginated, date range) | no |
| `who_posted_first` | Find which channel posted a news item first (by keyword) | no |

#### Category: interact
| Operation | Description | Destructive |
|-----------|-------------|-------------|
| `react_to_message` | Add emoji reaction to a message | no |
| `send_comment` | Post a comment on a channel message (in linked discussion group) | no |
| `forward_message` | Forward a message to Saved Messages or another chat | no |
| `mark_read` | Mark channel messages as read | no |

#### Category: folders
| Operation | Description | Destructive |
|-----------|-------------|-------------|
| `list_folders` | List all Telegram folders with channel counts | no |
| `folder_contents` | List channels in a specific folder | no |
| `move_to_folder` | Move a channel to a folder | no |
| `create_folder` | Create a new Telegram folder | no |

#### Category: analytics
| Operation | Description | Destructive |
|-----------|-------------|-------------|
| `compare_channels` | Compare 2+ channels by metrics (frequency, views, engagement) | no |
| `find_duplicates` | Find channels posting similar content (by text similarity) | no |
| `inactive_channels` | List channels with no posts in N days | no |
| `top_posts` | Find highest-engagement posts across channels in time window | no |
| `engagement_ranking` | Rank channels by engagement rate (reactions+comments / views) | no |

**Total Day 1: 23 operations** across 5 categories. More added as needs emerge.

---

## 5. Response Design

Applying all six AXI response principles (§6 of MCP design patterns guide).

### 5.1 TOON Format for Lists

All list responses use TOON (§2.4). Header declares field count and names, rows stream line by line.

```
channels[214]{name,handle,unread,posts_per_week}:
LLM Under the Hood,@llm_under_hood,12,4.2
TechSparks,@techsparks,8,5.1
```

**Rules:**
- Commas within field values escaped as `\,`
- Newlines within fields replaced with ` | ` (pipe-separated inline)
- Dates: ISO 8601 truncated (`2026-04-09T14:32`)
- Numbers: no formatting (no thousand separators)

### 5.2 Minimal Default Fields

Every list operation returns 3-4 essential fields by default. `fields` parameter for more.

| Operation | Default fields | Available fields |
|-----------|---------------|-----------------|
| `tg_feed` | text, date, views | + author, reactions, replies, forward_from, media_type, channel, message_id |
| `tg_overview` | name, unread, last_post | + handle, subscribers, posts_per_week, folder, description, engagement |

### 5.3 Content Truncation

Message text truncated at 300 chars by default. Truncation includes escape hatch:

```
RAG pipeline anti-patterns I've seen in prod this month...(truncated 1240 chars → include_full_text=true)
```

### 5.4 Pre-Computed Aggregates

Every list response starts or ends with a summary line:

```
summary: 15 messages from 8 channels | 24h window | avg 3.2K views
summary: 214 channels | 47 in folders | 3,284 total unread | 23 inactive >30d
```

### 5.5 Definitive Empty States

Never return empty output:

```
0 messages matching "AI governance" in last 24h across 214 channels.
Try: broader time window (hours=168), different keywords, or check specific channel with tg_feed channel=@handle
```

### 5.6 Next-Step Hints

Every response appends 2-3 contextually relevant next actions:

```
→ To read full message: tg_feed channel=@handle include_full_text=true
→ To react: tg_execute op="react_to_message" params={...}
→ To compare channels: tg_search_ops query="compare"
```

### 5.7 Response Mode Toggle

`response_format` parameter on `tg_execute`: `concise` (default, TOON) vs `detailed` (full JSON with all fields). ~33% reduction in concise mode.

---

## 6. Error Handling

Three-tier model from §7 of MCP design patterns guide.

### 6.1 Application Errors (`isError: true`)

```python
return {
    "isError": True,
    "content": [{
        "type": "text",
        "text": "Error: Channel @nonexistent not found.\n"
               "Expected: valid channel @handle, t.me link, or channel title.\n"
               "Example: @llm_under_hood or t.me/techsparks\n"
               "→ To find channels: tg_overview sort=name"
    }]
}
```

Every error includes: (1) what went wrong, (2) what was expected, (3) example, (4) recovery hint.

### 6.2 Telethon-Specific Errors

| Error | Handling |
|-------|----------|
| `FloodWaitError` | Return wait time: "Rate limited. Retry in {seconds}s. Telegram enforces this — cannot bypass." |
| `ChannelPrivateError` | "Channel is private or you were banned. Cannot access." |
| `ChatAdminRequiredError` | "Admin permissions required for this operation in {channel}." |
| `SessionPasswordNeededError` | "2FA enabled. Run `tg-mcp auth` in terminal to complete authentication." |
| `AuthKeyUnregisteredError` | "Session expired. Run `tg-mcp auth` to re-authenticate." |
| Network timeout | "Telegram API timeout after {n}s. This usually resolves itself — retry in a few seconds." |

### 6.3 Destructive Operation Safety

Operations marked `destructive=True` require `confirm=true` parameter:

```
tg_execute op="unsubscribe" params={"channel": "@spam_channel"}
→ "This will unsubscribe you from @spam_channel (12,340 subscribers). Pass confirm=true to proceed."

tg_execute op="unsubscribe" params={"channel": "@spam_channel"} confirm=true
→ "Unsubscribed from @spam_channel. To re-subscribe: tg_execute op='subscribe' params={'channel': '@spam_channel'}"
```

### 6.4 Fail-Fast Validation

Validate all parameters before making Telegram API calls:
- Channel handles: must start with `@` or be a valid `t.me/` link or title substring
- Limits: enforce min/max ranges (1-100 for messages, 1-500 for channels)
- Date ranges: max 720 hours (30 days) for feed, no limit for history download
- Emoji: validate against Telegram's supported reaction emoji set

---

## 7. Token Efficiency

Target: minimal context footprint, fast responses.

### 7.1 Static Tool Budget

5 tools × ~150 tokens each = **~750 tokens** total MCP schema footprint. Compare: GitHub MCP (93 tools) = 55,000 tokens. Our approach: **98.6% reduction**.

### 7.2 Dynamic Catalog Cost

Catalog discovery is on-demand:
- `tg_search_ops` response: ~50-100 tokens (list of names + one-liners)
- `tg_describe_op` response: ~100-200 tokens (full schema for one op)
- Total for discover+describe+execute flow: ~200-400 tokens overhead
- vs loading all 23 operations statically: ~3,500+ tokens always in context

### 7.3 Response Efficiency

| Technique | Reduction | Applied where |
|-----------|-----------|--------------|
| TOON format | 30-60% vs JSON | All list responses |
| Minimal default fields (3-4) | ~50% vs all fields | tg_feed, tg_overview |
| Text truncation (300 chars) | Variable | Message text in feeds |
| Pre-computed aggregates | Saves follow-up calls | Every list response |
| Concise response mode | ~33% | tg_execute default |

### 7.4 SQLite Cache

Cache avoids repeated API calls within a session:

| Data | TTL | Why |
|------|-----|-----|
| Channel list + metadata | 1 hour | Rarely changes |
| Channel subscriber count | 6 hours | Slow-moving metric |
| Messages | 15 minutes | Fresh enough for on-demand |
| Folder structure | 1 hour | Rarely changes |
| User's own reactions | No cache | Must be real-time |

Cache key: operation name + serialized params. Cache invalidated on write operations.

---

## 8. Telethon Core

### 8.1 Client Lifecycle

```
MCP Server starts
  → Check session file exists
    → Yes: Connect with existing session
    → No: Return error "Run tg-mcp auth to authenticate"
  → Telethon client connected (lazy — on first tool call, not on MCP start)
  → Client stays alive for MCP server lifetime
  → MCP server stops → Telethon disconnects gracefully
```

**Lazy connection:** Don't connect to Telegram on MCP server startup. Connect on first actual tool call. This avoids connection overhead when Claude Code loads the MCP server but doesn't use Telegram tools.

### 8.2 Authentication (One-Time Setup)

Separate CLI command, not part of MCP server:

```bash
cd ~/.tg-mcp && python -m tg_mcp.auth
# Prompts for phone number, sends code via Telegram, optional 2FA password
# Creates session.session file
# Done once, session persists across restarts
```

### 8.3 Rate Limiting

Telethon handles most rate limiting automatically. Additional safety:
- Max 30 API calls per second (Telegram's hard limit)
- FloodWaitError: wait the required time, return estimated wait to Claude
- Batch operations (e.g., stats for 200 channels): process in chunks of 20 with 1s delay

### 8.4 Connection Resilience

- Auto-reconnect on connection drop (Telethon built-in)
- Timeout per API call: 30 seconds
- If Telegram is unreachable for >60s: return error, don't block MCP

---

## 9. SQLite Schema

```sql
-- Channel index (cached from Telegram)
CREATE TABLE channels (
    id INTEGER PRIMARY KEY,          -- Telegram channel ID
    title TEXT NOT NULL,
    handle TEXT,                      -- @handle, may be NULL
    subscribers INTEGER,
    is_channel BOOLEAN DEFAULT TRUE,  -- channel vs group
    folder TEXT,
    last_post_date TEXT,              -- ISO 8601
    posts_per_week REAL,
    unread_count INTEGER DEFAULT 0,
    cached_at TEXT NOT NULL           -- ISO 8601, for TTL
);

-- Message cache
CREATE TABLE messages (
    id INTEGER PRIMARY KEY,           -- Telegram message ID
    channel_id INTEGER NOT NULL,
    date TEXT NOT NULL,               -- ISO 8601
    text TEXT,
    author TEXT,
    views INTEGER DEFAULT 0,
    reactions_json TEXT,              -- JSON: {"👍": 12, "🔥": 5}
    replies INTEGER DEFAULT 0,
    forward_from TEXT,
    media_type TEXT,                  -- photo, video, document, none
    cached_at TEXT NOT NULL,
    FOREIGN KEY (channel_id) REFERENCES channels(id)
);

-- Folder structure
CREATE TABLE folders (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    channel_ids_json TEXT,           -- JSON array of channel IDs
    cached_at TEXT NOT NULL
);

-- Cache metadata
CREATE TABLE cache_meta (
    key TEXT PRIMARY KEY,            -- "channels_list", "feed_@handle_24h", etc.
    cached_at TEXT NOT NULL,
    ttl_seconds INTEGER NOT NULL
);

CREATE INDEX idx_messages_channel_date ON messages(channel_id, date DESC);
CREATE INDEX idx_messages_date ON messages(date DESC);
CREATE INDEX idx_channels_handle ON channels(handle);
CREATE INDEX idx_channels_folder ON channels(folder);
```

**WAL mode enabled** for concurrent read access. Single writer (MCP server process).

---

## 10. File Structure

```
/Users/todmy/github/telegram-mcp/
├── docs/
│   └── spec.md                    # This file
├── src/
│   └── tg_mcp/
│       ├── __init__.py
│       ├── __main__.py            # Entry: python -m tg_mcp (starts MCP server)
│       ├── server.py              # MCP server — 5 static tools, stdio transport
│       ├── client.py              # Telethon client wrapper (lazy connect, reconnect)
│       ├── cache.py               # SQLite cache layer (read/write, TTL checks)
│       ├── catalog.py             # Operations registry, search, describe, execute
│       ├── toon.py                # TOON formatter (list → TOON string)
│       ├── config.py              # Settings from .env + defaults
│       ├── auth.py                # CLI auth flow (python -m tg_mcp.auth)
│       ├── ops/                   # Operations catalog
│       │   ├── __init__.py        # Auto-imports all modules in ops/
│       │   ├── channels.py        # list, info, stats, subscribe, unsubscribe, mute
│       │   ├── messages.py        # search, get, history, who_posted_first
│       │   ├── interact.py        # react, comment, forward, mark_read
│       │   ├── folders.py         # list, contents, move, create
│       │   └── analytics.py       # compare, duplicates, inactive, top_posts, ranking
│       └── db/
│           ├── __init__.py
│           ├── schema.sql         # SQLite DDL
│           └── migrations.py      # Schema versioning (simple version table)
├── tests/
│   ├── test_catalog.py            # Operation registration + search
│   ├── test_toon.py               # TOON formatter
│   ├── test_cache.py              # Cache TTL + invalidation
│   └── test_tools.py              # MCP tool responses
├── pyproject.toml                 # Project config, dependencies
├── .env.example                   # Template: API_ID, API_HASH, PHONE
├── .gitignore                     # .env, session.session, *.db, __pycache__
├── README.md                      # Setup instructions
└── CLAUDE.md                      # Claude Code project instructions
```

---

## 11. Dependencies

```toml
[project]
name = "tg-mcp"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "mcp>=1.0",                    # MCP Python SDK
    "telethon>=1.36",              # Telegram User API client
    "aiosqlite>=0.20",             # Async SQLite
    "python-dotenv>=1.0",          # .env loading
    "cryptg>=0.4",                 # Faster Telegram crypto (optional but recommended)
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
]
```

---

## 12. Claude Code Integration

### 12.1 MCP Server Registration

Add to `~/.claude/settings.json` or project `.mcp.json`:

```json
{
  "mcpServers": {
    "telegram": {
      "command": "python",
      "args": ["-m", "tg_mcp"],
      "cwd": "/Users/todmy/github/godmode-telegram-mcp",
      "env": {
        "TG_MCP_DATA_DIR": "~/.tg-mcp"
      }
    }
  }
}
```

### 12.2 Data Directory

```
~/.tg-mcp/
├── session.session          # Telethon session (created by auth)
├── cache.db                 # SQLite cache
├── .env                     # API_ID, API_HASH, PHONE
└── logs/                    # Optional, structured logs
```

### 12.3 First-Time Setup

```bash
# 1. Get API credentials from my.telegram.org
# 2. Create config
cp /Users/todmy/github/telegram-mcp/.env.example ~/.tg-mcp/.env
# Edit .env with API_ID, API_HASH, PHONE

# 3. Authenticate (one-time)
cd /Users/todmy/github/telegram-mcp && python -m tg_mcp.auth

# 4. MCP server auto-starts when Claude Code calls a tg_* tool
```

---

## 13. Anti-Patterns Avoided (§9 Checklist)

| Anti-Pattern | How We Avoid It |
|-------------|----------------|
| Full-API Trap (§9.1) | Manual curation — 23 operations selected for real use cases, not Telethon API 1:1 |
| Excessive Tool Count (§9.2) | 5 static tools. Dynamic catalog for the rest |
| Operation-Oriented Design (§9.3) | Outcome-oriented: `who_posted_first`, `find_duplicates`, `engagement_ranking` — not `get_messages` + `count` + `sort` |
| Poor Tool Descriptions (§9.4) | 6-component rubric applied: purpose, guidelines, limitations, params, length, alternatives |
| Loading All Schemas (§9.5) | Dynamic toolsets — only 5 schemas loaded, rest on demand |
| Generic Error Messages (§9.6) | Every error: what happened + expected + example + recovery hint |
| Excessive Response Data (§9.7) | TOON + minimal fields + truncation + aggregates |
| Monolithic Server (§9.8) | Single domain (Telegram), but internal decomposition by category |

---

## 14. Security

| Concern | Mitigation |
|---------|-----------|
| API credentials | `.env` file, never in code. `.gitignore`'d |
| Session file | `~/.tg-mcp/session.session`, permission 600. Contains auth token — treat as password |
| Destructive operations | `confirm=true` required. MCP tool annotation `destructiveHint: true` |
| Input validation | All params validated before Telegram API calls (Pydantic or manual) |
| Rate limiting | Built-in FloodWait respect + 30 calls/sec ceiling |
| No command injection | No shell calls. Pure Python → Telethon API |

---

## 15. Future Extensions (Not in Scope Now)

Documented for architectural awareness, not for implementation:

- **Background sync daemon** — periodic channel polling, push notifications for high-engagement posts
- **Shared Telethon core** with Telegram Claude Bot project
- **Embedding-based duplicate detection** — find channels posting semantically similar content
- **Auto-scoring model** — learned from user's read/skip patterns
- **Export to Obsidian** — save interesting posts as notes
- **Telegram Claude Bot** — the reverse direction (Telegram → Claude), shares Telethon core

---

## 16. MCP Design Patterns Checklist (from guide §11)

### Tool Design
- [x] Total tool count ≤ 15 → **5 static tools**
- [x] Tools are outcome-oriented, not operation-oriented
- [x] One server = one domain (Telegram)
- [x] Tool names: `tg_` prefix, verb_noun format
- [x] Tool annotations: readOnlyHint, destructiveHint, idempotentHint

### Tool Descriptions
- [x] Each description: purpose + guidelines + limitations
- [x] Compact and targeted
- [x] Alternative tools mentioned
- [x] Parameter descriptions with format + constraints
- [x] No vague language

### Response Design
- [x] Minimal fields by default (3-4), `fields` param for more
- [x] Long text truncated with size hints + escape hatch
- [x] Pre-computed aggregates (counts, summaries)
- [x] Empty states explicit
- [x] Next-step hints appended
- [x] TOON format for list responses

### Error Handling
- [x] `isError: true` for application errors
- [x] Error: what happened + expected + example
- [x] Destructive ops require confirmation
- [x] No interactive prompts
- [x] Fail-fast validation

### Token Efficiency
- [x] Schema overhead: ~750 tokens (5 tools)
- [x] Dynamic loading for operations catalog
- [x] TOON for list responses
- [x] Server-side filtering before response
- [x] Minimal default fields

### Security
- [x] Input validation on all parameters
- [x] No command injection vectors
- [x] API keys in env vars
- [x] Destructive operations require confirmation
