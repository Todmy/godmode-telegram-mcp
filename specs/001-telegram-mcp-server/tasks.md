# Tasks: Telegram MCP Server

**Input**: Design documents from `/specs/001-telegram-mcp-server/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/mcp-tools.md

**Tests**: Not explicitly requested in spec. Test tasks omitted.

**Organization**: Tasks grouped by user story (P1 → P2 → P3). Each story is independently testable after foundational phase.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story (US1–US7)
- All paths relative to repo root

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization, packaging, configuration

- [x] T001 Create project structure: pyproject.toml with dependencies (mcp, telethon, aiosqlite, python-dotenv, cryptg), src/tg_mcp/__init__.py, src/tg_mcp/__main__.py entry point
- [x] T002 [P] Create .env.example (API_ID, API_HASH, PHONE) and update .gitignore (.env, session.session, *.db, __pycache__, ~/.tg-mcp/)
- [x] T003 [P] Implement config loader and logging setup in src/tg_mcp/config.py — load from ~/.tg-mcp/.env via python-dotenv, expose API_ID, API_HASH, PHONE, DATA_DIR with defaults. Configure structured logging (JSON format) to ~/.tg-mcp/logs/ with rotation. Export shared logger for use across all modules (FR-024)

**Checkpoint**: Project installable via `pip install -e .`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that ALL user stories depend on

**CRITICAL**: No user story work can begin until this phase is complete

- [x] T004 Create SQLite DDL in src/tg_mcp/db/schema.sql — channels, messages, folders, cache_meta tables with indexes per data-model.md
- [x] T005 [P] Implement schema migrations in src/tg_mcp/db/__init__.py and src/tg_mcp/db/migrations.py — version table, WAL mode pragma, schema apply on first connect. Note: references schema.sql from T004 at runtime; can be written in parallel but both must complete before T008
- [x] T006 [P] Implement TOON formatter in src/tg_mcp/toon.py — header generation (type[count]{fields}:), row serialization, comma escaping, date formatting, summary line helper, next-step hints helper
- [x] T007 [P] Implement Telethon client wrapper in src/tg_mcp/client.py — lazy connect on first call, session from ~/.tg-mcp/session.session, auto-reconnect, FloodWaitError handling (wait + return duration), 30s timeout, graceful disconnect
- [x] T008 Implement SQLite cache layer in src/tg_mcp/cache.py — async read/write via aiosqlite, TTL check per data type (channels 1h, messages 15min, folders 1h, subscribers 6h), cache key generation (op+params), invalidation on write ops
- [x] T009 Implement operations catalog in src/tg_mcp/catalog.py — @operation() decorator (name, category, description, destructive, idempotent), registry dict, search by keyword+category, describe via type hint introspection, execute dispatch with param validation and confirm gate
- [x] T010 Create src/tg_mcp/ops/__init__.py — auto-import all .py modules in ops/ directory to trigger @operation() registration
- [x] T011 Implement MCP server shell in src/tg_mcp/server.py — 5 tool registrations (tg_feed, tg_overview, tg_search_ops, tg_describe_op, tg_execute) with annotations (readOnlyHint, idempotentHint, destructiveHint), stdio transport, structured error helper (4-part: what/expected/example/recovery), isError flag. Use shared logger from config.py (T003)

**Checkpoint**: Foundation ready — `python -m tg_mcp` starts MCP server, tools are registered (stubs), catalog accepts @operation() decorators

---

## Phase 3: User Story 1 — Read Channel Feed (Priority: P1) MVP

**Goal**: Fetch recent messages from subscribed channels with configurable time window, field selection, and TOON-formatted responses

**Independent Test**: Request feed from a single channel → verify text, date, views returned within 5s

- [x] T012 [US1] Implement channel resolution in src/tg_mcp/client.py — resolve @handle, t.me link, or title substring to Telethon entity. On substring multi-match: return all matching channels (FR-023). Validate handle format
- [x] T013 [US1] Implement tg_feed tool logic in src/tg_mcp/server.py — fetch messages via client.py for resolved channel(s), apply hours filter (default 24, max 720), limit (default 20, max 100), fields selection (default: text, date, views), folder filter, include_full_text toggle
- [x] T014 [US1] Wire feed response through toon.py — truncate text at 300 chars with "(truncated N chars → include_full_text=true)" hint, assemble summary line (count, channels, window, avg views), append next-step hints (full text, search, channel stats). Cache messages via cache.py (15min TTL)

**Checkpoint**: `tg_feed channel=@example hours=48` returns TOON-formatted messages with truncation and hints

---

## Phase 4: User Story 2 — Channel Overview & Discovery (Priority: P1)

**Goal**: List all subscribed channels with activity metrics, sorting, and filtering

**Independent Test**: Request overview → verify channel names, unread counts, last post dates returned

- [x] T015 [US2] Implement tg_overview tool logic in src/tg_mcp/server.py — fetch all subscribed channels via client.py, populate metrics (unread, subscribers, posts_per_week, last_post_date), apply sort (default: unread; options: activity, subscribers, name, last_post), type filter (channels/groups/all), folder filter, min_subscribers filter, limit (default 50, max 500), fields selection
- [x] T016 [US2] Wire overview response through toon.py — assemble summary line (total channels, folders count, total unread, inactive count), append next-step hints (read messages, manage subscriptions, manage folders). Cache channel list via cache.py (1h TTL, 6h for subscriber counts)

**Checkpoint**: `tg_overview sort=activity folder="AI News"` returns sorted TOON channel list with summary

---

## Phase 5: User Story 3 — Discover & Execute Operations (Priority: P1)

**Goal**: Search catalog by keyword, view operation schemas, execute operations with parameter validation and destructive-op confirmation

**Independent Test**: Search "react" → get schema → execute react_to_message successfully

- [x] T017 [US3] Implement tg_search_ops tool logic in src/tg_mcp/server.py — search catalog.py registry by keyword (name + description match) and optional category filter, format as "ops[N] matching 'query':" with name-description pairs, definitive empty state on 0 matches, next-step hints
- [x] T018 [US3] Implement tg_describe_op tool logic in src/tg_mcp/server.py — look up operation by name in catalog, format schema as structured text (op name, description, category, destructive, idempotent, params with types/required/defaults, returns, example invocation), error if not found (4-part)
- [x] T019 [US3] Implement tg_execute tool logic in src/tg_mcp/server.py — look up op in catalog, validate params against schema, check confirm=true for destructive ops (return warning if missing), dispatch to operation function, format response per response_format (concise=TOON, detailed=full), handle all error cases (4-part format)
- [x] T020 [P] [US3] Implement read-only channel operations in src/tg_mcp/ops/channels.py — list_channels (list all with basic info), channel_info (detailed single channel: description, admins, creation date), channel_stats (activity: post frequency, avg views, engagement rate, growth over N days). Register via @operation() decorator

**Checkpoint**: Full discover→describe→execute flow works. `tg_search_ops query="channel"` → `tg_describe_op name="channel_stats"` → `tg_execute op="channel_stats" params={"channel":"@example"}`

---

## Phase 6: User Story 4 — Search Messages Across Channels (Priority: P2)

**Goal**: Keyword search across all channels with date filtering. Find which channel posted a story first.

**Independent Test**: Search keyword → verify matches from multiple channels with source attribution

- [x] T021 [P] [US4] Implement search_messages operation in src/tg_mcp/ops/messages.py — keyword search via Telethon global search across subscribed channels, date range filter (from/to), limit, return messages with channel attribution, sorted by relevance. TOON response
- [x] T022 [P] [US4] Implement get_message operation in src/tg_mcp/ops/messages.py — fetch single message by channel + message_id, return full content with all fields including media metadata (type, filename, size, download link per FR-022)
- [x] T023 [US4] Implement message_history operation in src/tg_mcp/ops/messages.py — paginated download of channel message history with date range, offset-based pagination, TOON response
- [x] T024 [US4] Implement who_posted_first operation in src/tg_mcp/ops/messages.py — search keyword across channels, group by channel, sort by earliest match date, return chronological list of channels with timestamps

**Checkpoint**: `tg_execute op="search_messages" params={"query":"AI governance","hours":168}` returns cross-channel results

---

## Phase 7: User Story 5 — Interact with Content (Priority: P2)

**Goal**: React, comment, forward messages, and mark channels as read

**Independent Test**: React to a message → verify reaction applied with updated count

- [x] T025 [P] [US5] Implement react_to_message operation in src/tg_mcp/ops/interact.py — add emoji reaction to message by channel + message_id, validate emoji against Telegram supported set, return confirmation with updated reaction count
- [x] T026 [P] [US5] Implement send_comment operation in src/tg_mcp/ops/interact.py — post comment text on channel message in linked discussion group, return confirmation with comment ID
- [x] T027 [P] [US5] Implement forward_message operation in src/tg_mcp/ops/interact.py — forward message to Saved Messages (default) or specified chat, return confirmation
- [x] T028 [US5] Implement mark_read operation in src/tg_mcp/ops/interact.py — mark all messages in channel as read, invalidate unread cache, return confirmation with previous unread count

**Checkpoint**: `tg_execute op="react_to_message" params={"channel":"@example","message_id":123,"emoji":"fire"}` succeeds

---

## Phase 8: User Story 6 — Manage Folders & Subscriptions (Priority: P3)

**Goal**: Create/manage folders, move channels between folders, subscribe/unsubscribe with confirmation

**Independent Test**: Create folder → move channel into it → verify folder contents

- [x] T029 [P] [US6] Implement list_folders and folder_contents operations in src/tg_mcp/ops/folders.py — list all folders with channel counts (TOON), list channels in specific folder with metrics
- [x] T030 [P] [US6] Implement move_to_folder and create_folder operations in src/tg_mcp/ops/folders.py — move channel to folder (invalidate folder cache), create new folder by name, return confirmation
- [x] T031 [US6] Implement subscribe, unsubscribe, and mute_channel operations in src/tg_mcp/ops/channels.py — subscribe by handle/link, unsubscribe with destructive=True + confirm gate (show subscriber count in warning, include re-subscribe hint on success), mute/unmute notifications. Invalidate channel cache on changes

**Checkpoint**: `tg_execute op="unsubscribe" params={"channel":"@spam"}` → warning → `confirm=true` → success with re-subscribe hint

---

## Phase 9: User Story 7 — Channel Analytics & Comparison (Priority: P3)

**Goal**: Compare channels, find inactive/duplicate channels, top posts, engagement ranking

**Independent Test**: Compare two channels → verify side-by-side metrics returned

- [x] T032 [P] [US7] Implement compare_channels operation in src/tg_mcp/ops/analytics.py — accept 2+ channel handles, fetch metrics (post frequency, avg views, subscribers, engagement rate) for each, return side-by-side TOON comparison
- [x] T033 [P] [US7] Implement find_duplicates operation in src/tg_mcp/ops/analytics.py — compare recent messages across channels by text similarity (substring/keyword overlap), group channels posting similar content, return duplicate pairs with similarity score
- [x] T034 [P] [US7] Implement inactive_channels operation in src/tg_mcp/ops/analytics.py — find channels with no posts in N days (default 30), return list sorted by last post date, TOON format
- [x] T035 [US7] Implement top_posts and engagement_ranking operations in src/tg_mcp/ops/analytics.py — top_posts: highest-engagement messages across channels in time window (by reactions+comments+views). engagement_ranking: rank channels by engagement rate (reactions+comments / views), return TOON lists

**Checkpoint**: `tg_execute op="compare_channels" params={"channels":["@ch1","@ch2"]}` returns comparison table

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Auth setup, documentation, end-to-end validation

- [x] T036 [P] Implement auth CLI in src/tg_mcp/auth.py — interactive phone + code + optional 2FA flow, create session file at ~/.tg-mcp/session.session with permission 600, invoked via `python -m tg_mcp.auth`
- [x] T037 [P] Create README.md — project description, prerequisites (Python 3.11+, Telegram API creds), setup steps (install, configure, auth, register MCP), usage examples, troubleshooting table
- [x] T038 Run quickstart.md validation — verify setup flow end-to-end: install → configure → auth → tg_overview → tg_feed → tg_search_ops → tg_execute. Fix any issues discovered

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1, US2, US3 (Phases 3-5)**: All depend on Foundational. Can run in parallel after Phase 2
- **US4, US5 (Phases 6-7)**: Depend on US3 (catalog + execute flow). Can run in parallel with each other
- **US6, US7 (Phases 8-9)**: Depend on US3. Can run in parallel with each other and with US4/US5
- **Polish (Phase 10)**: Depends on all user stories being complete

### User Story Dependencies

- **US1 (P1)**: After Foundational — no story dependencies
- **US2 (P1)**: After Foundational — no story dependencies
- **US3 (P1)**: After Foundational — no story dependencies (but needs at least one op; T020 provides channel ops)
- **US4 (P2)**: After US3 (needs tg_execute to be functional)
- **US5 (P2)**: After US3 (needs tg_execute to be functional)
- **US6 (P3)**: After US3 (needs tg_execute + confirm gate)
- **US7 (P3)**: After US3 (needs tg_execute to be functional)

### Within Each User Story

- Models/entities before services
- Core implementation before response formatting
- Story complete before moving to next priority

### Parallel Opportunities

- T002, T003: parallel (different files)
- T005, T006, T007: parallel (different files)
- T020: parallel with T017-T019 (different file: ops/channels.py vs server.py)
- T021, T022: parallel (same file but independent functions)
- T025, T026, T027: parallel (same file but independent functions)
- T029, T030: parallel (same file but independent functions)
- T032, T033, T034: parallel (same file but independent functions)
- T036, T037: parallel (different files)

---

## Implementation Strategy

### MVP First (US1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: US1 — Read Channel Feed
4. **STOP and VALIDATE**: `tg_feed` works end-to-end with real Telegram data
5. Deploy — server is usable for channel reading

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. US1 (Feed) → Core value: read channels
3. US2 (Overview) → Orientation: what channels do I have
4. US3 (Dynamic Toolsets) → Extensibility: discover + execute anything
5. US4 (Search) + US5 (Interact) → Intelligence + engagement (parallel)
6. US6 (Folders) + US7 (Analytics) → Organization + power features (parallel)
7. Polish → Auth CLI, docs, validation

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to user story for traceability
- No test tasks generated (not requested in spec)
- Commit after each task or logical group
- All code must be async — Constitution Principle II
- All list responses must use TOON — Constitution Principle I
- All errors must use 4-part format — Constitution Principle VI
