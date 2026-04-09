"""MCP server — 5 static tools, stdio transport.

Tools:
    tg_feed       — Read channel messages (readOnly, idempotent)
    tg_overview   — Channel/folder overview (readOnly, idempotent)
    tg_search_ops — Discover operations (readOnly, idempotent)
    tg_describe_op — Get operation schema (readOnly, idempotent)
    tg_execute    — Run any operation (conservative: not readOnly, not idempotent)

All responses use TextContent. Errors use the 4-part format:
what happened / expected / example / recovery hint.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, Chat

from tg_mcp import catalog, toon
from tg_mcp.cache import Cache
from tg_mcp.catalog import OperationError
from tg_mcp.client import (
    ChannelResolutionError,
    TelegramClient,
    TelegramConnectionError,
    TelegramFloodWait,
)
from tg_mcp.config import ConfigError, Settings, load_settings, logger
from tg_mcp.db import get_db

# ---------------------------------------------------------------------------
# Structured error helper
# ---------------------------------------------------------------------------


def _error_text(
    what: str,
    expected: str,
    example: str,
    recovery: str,
) -> str:
    """Build a 4-part structured error message."""
    return (
        f"Error: {what}\n"
        f"Expected: {expected}\n"
        f"Example: {example}\n"
        f"\u2192 {recovery}"
    )


# ---------------------------------------------------------------------------
# Module-level state (initialized in run_server)
# ---------------------------------------------------------------------------

_settings: Settings | None = None
_tg_client: TelegramClient | None = None
_cache: Cache | None = None

# Create the FastMCP server instance
mcp = FastMCP(
    "tg-mcp",
    instructions="Telegram MCP server — full client capabilities for Claude Code",
)


# ---------------------------------------------------------------------------
# Tool annotations
# ---------------------------------------------------------------------------

_READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    idempotentHint=True,
    destructiveHint=False,
    openWorldHint=False,
)

_READ_ONLY_OPEN = ToolAnnotations(
    readOnlyHint=True,
    idempotentHint=True,
    destructiveHint=False,
    openWorldHint=True,
)

_EXECUTE = ToolAnnotations(
    readOnlyHint=False,
    idempotentHint=False,
    destructiveHint=False,
    openWorldHint=True,
)


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------


@mcp.tool(
    name="tg_feed",
    description=(
        "Fetch recent messages from one or more Telegram channels/groups. "
        "Returns messages with author, date, text, views, reactions, reply count. "
        "For single-channel deep dive, use channel handle. "
        "For multi-channel digest, omit channel to get cross-channel feed sorted by time. "
        "Truncates message text at 300 chars by default \u2014 use include_full_text=true for complete content. "
        "For searching by keyword across channels, use tg_search_ops to find the search operation instead."
    ),
    annotations=_READ_ONLY_OPEN,
)
async def tg_feed(
    channel: str | None = None,
    limit: int = 20,
    hours: int = 24,
    fields: list[str] | None = None,
    include_full_text: bool = False,
    folder: str | None = None,
) -> str:
    """Read channel messages."""
    errors = _validate_feed_params(limit, hours)
    if errors:
        return errors

    # Validate fields early — reject unknown field names before any API call
    all_fields = {
        "text", "date", "views", "author", "reactions", "replies",
        "forward_from", "media_type", "channel", "message_id",
    }
    default_fields = ["text", "date", "views"]
    selected_fields = default_fields

    if fields is not None:
        unknown = [f for f in fields if f not in all_fields]
        if unknown:
            return _error_text(
                f"Unknown field(s): {', '.join(unknown)}",
                f"valid fields: {', '.join(sorted(all_fields))}",
                'tg_feed fields=["text", "date", "views", "author"]',
                "use only recognized field names",
            )
        selected_fields = list(fields) if fields else default_fields

    # Always include 'channel' in multi-channel feed for identification
    if channel is None and "channel" not in selected_fields:
        selected_fields = ["channel"] + selected_fields

    # Ensure client is initialized
    if _tg_client is None:
        return _error_text(
            "Telegram client not initialized",
            "server started with valid config",
            "python -m tg_mcp",
            "check ~/.tg-mcp/.env configuration",
        )

    try:
        tg = await _tg_client.get()
    except TelegramConnectionError as exc:
        return _error_text(
            str(exc),
            "connected Telegram session",
            "python -m tg_mcp.auth",
            "run auth command or check network connectivity",
        )
    except TelegramFloodWait as exc:
        return _error_text(
            f"Rate limited by Telegram. Wait {exc.seconds}s.",
            "no rate limiting",
            "retry after the wait period",
            f"wait {exc.seconds}s before retrying",
        )

    # Resolve target channel(s)
    try:
        entities = await _resolve_feed_channels(channel, folder)
    except ChannelResolutionError as exc:
        return _error_text(
            str(exc),
            "valid channel @handle, t.me link, or title substring",
            'tg_feed channel="@llm_under_hood"',
            "use tg_overview to see your subscribed channels",
        )
    except TelegramFloodWait as exc:
        return _error_text(
            f"Rate limited during channel resolution. Wait {exc.seconds}s.",
            "no rate limiting",
            "retry after the wait period",
            f"wait {exc.seconds}s before retrying",
        )

    if not entities:
        return toon.empty_state(
            "channels",
            "matching your filter",
            ["remove folder filter", "check channel name spelling"],
        )

    # Fetch messages from all target channels
    cutoff = datetime.now(timezone.utc).timestamp() - (hours * 3600)
    all_messages: list[dict[str, Any]] = []

    assert _settings is not None
    db = await get_db(_settings.db_path)

    for entity in entities:
        entity_id = entity.id
        entity_title = getattr(entity, "title", "") or ""
        entity_handle = getattr(entity, "username", None)
        handle_display = f"@{entity_handle}" if entity_handle else entity_title

        # Check cache first
        if _cache is not None:
            cached = await _cache.get_messages(db, entity_id, limit=limit)
            if cached is not None:
                for msg in cached:
                    msg["_channel_title"] = handle_display
                all_messages.extend(cached)
                continue

        # Fetch from Telegram
        try:
            fetched = await _fetch_channel_messages(
                tg, entity, entity_id, handle_display, limit, cutoff
            )
        except TelegramFloodWait as exc:
            return _error_text(
                f"Rate limited fetching {handle_display}. Wait {exc.seconds}s.",
                "no rate limiting",
                "retry after the wait period",
                f"wait {exc.seconds}s before retrying",
            )
        except Exception as exc:
            logger.exception(
                "server.feed_fetch_error",
                extra={"channel": handle_display},
            )
            return _error_text(
                f"Failed to fetch messages from {handle_display}: "
                f"{type(exc).__name__}: {exc}",
                "successful message fetch",
                f'tg_feed channel="{handle_display}"',
                "check channel access and retry",
            )

        # Cache fetched messages
        if _cache is not None and fetched:
            await _cache.put_messages(db, entity_id, fetched)

        all_messages.extend(fetched)

    # Filter by time window
    all_messages = [
        m for m in all_messages
        if _msg_timestamp(m) >= cutoff
    ]

    # Sort by date descending and apply limit
    all_messages.sort(key=lambda m: _msg_timestamp(m), reverse=True)
    all_messages = all_messages[:limit]

    # Handle empty result
    if not all_messages:
        channel_desc = channel or "all subscribed channels"
        return toon.empty_state(
            "messages",
            f"in {channel_desc} in last {hours}h",
            [
                f"broaden time window: hours={min(hours * 2, 720)}",
                "check specific channel: tg_feed channel=@handle",
                "see all channels: tg_overview",
            ],
        )

    # Build TOON response
    toon_rows: list[list[Any]] = []
    channel_set: set[str] = set()

    for msg in all_messages:
        row_values: list[Any] = []
        ch_name = msg.get("_channel_title", "")
        channel_set.add(ch_name)

        for f in selected_fields:
            if f == "text":
                text = msg.get("text") or ""
                if not include_full_text and len(text) > 300:
                    truncated_len = len(text) - 300
                    text = (
                        f"{text[:300]}...(truncated {truncated_len} chars "
                        f"\u2192 include_full_text=true)"
                    )
                row_values.append(text)
            elif f == "date":
                row_values.append(toon.format_date(msg.get("date")))
            elif f == "views":
                row_values.append(msg.get("views", 0))
            elif f == "author":
                row_values.append(msg.get("author") or "")
            elif f == "reactions":
                reactions = msg.get("reactions") or {}
                if isinstance(reactions, dict) and reactions:
                    row_values.append(
                        " ".join(f"{e}{c}" for e, c in reactions.items())
                    )
                else:
                    row_values.append("")
            elif f == "replies":
                row_values.append(msg.get("replies", 0))
            elif f == "forward_from":
                row_values.append(msg.get("forward_from") or "")
            elif f == "media_type":
                row_values.append(msg.get("media_type") or "")
            elif f == "channel":
                row_values.append(ch_name)
            elif f == "message_id":
                row_values.append(msg.get("id", ""))

        toon_rows.append(row_values)

    # Summary and hints
    total_views = sum(m.get("views", 0) for m in all_messages)
    avg_views = total_views // len(all_messages) if all_messages else 0
    summary_parts = [
        f"{len(all_messages)} messages",
        f"from {len(channel_set)} channel{'s' if len(channel_set) != 1 else ''}",
        f"{hours}h window",
    ]
    if avg_views > 0:
        summary_parts.append(f"avg {_format_compact_number(avg_views)} views")

    next_hints = []
    if not include_full_text:
        next_hints.append(
            "For full message: tg_feed channel=@handle include_full_text=true"
        )
    next_hints.append('To search by keyword: tg_search_ops query="search messages"')
    next_hints.append('To see channel stats: tg_search_ops query="channel statistics"')

    return toon.format_response(
        type_name="feed",
        fields=selected_fields,
        rows=toon_rows,
        summary_parts=summary_parts,
        next_hints=next_hints,
    )


@mcp.tool(
    name="tg_overview",
    description=(
        "Overview of subscribed channels, groups, and folders with activity metrics. "
        "Returns channel list with subscriber count, post frequency, unread count, and last post date. "
        "Default sort: by unread count (most unread first). Use sort parameter to change. "
        "For detailed stats on a single channel, use tg_search_ops to find the channel_stats operation. "
        'For managing folders (create, move channels), use tg_search_ops query="folders".'
    ),
    annotations=_READ_ONLY_OPEN,
)
async def tg_overview(
    sort: str = "unread",
    folder: str | None = None,
    min_subscribers: int = 0,
    type: str = "all",
    limit: int = 50,
    fields: list[str] | None = None,
) -> str:
    """Channel/folder overview."""
    valid_sorts = {"unread", "activity", "subscribers", "name", "last_post"}
    if sort not in valid_sorts:
        return _error_text(
            f"Invalid sort value: {sort!r}",
            f"one of: {', '.join(sorted(valid_sorts))}",
            'tg_overview sort="activity"',
            "use one of the listed sort options",
        )

    valid_types = {"channels", "groups", "all"}
    if type not in valid_types:
        return _error_text(
            f"Invalid type filter: {type!r}",
            f"one of: {', '.join(sorted(valid_types))}",
            'tg_overview type="channels"',
            "use 'channels', 'groups', or 'all'",
        )

    if limit < 1 or limit > 500:
        return _error_text(
            f"limit must be 1-500, got: {limit}",
            "integer between 1 and 500",
            "tg_overview limit=100",
            "use a value in the valid range",
        )

    if min_subscribers < 0:
        return _error_text(
            f"min_subscribers must be non-negative, got: {min_subscribers}",
            "integer >= 0",
            "tg_overview min_subscribers=1000",
            "use 0 or a positive number",
        )

    # Validate fields
    all_fields = {
        "name", "handle", "subscribers", "unread", "last_post",
        "posts_per_week", "folder", "description",
    }
    default_fields = ["name", "unread", "last_post"]
    selected_fields = default_fields

    if fields is not None:
        unknown = [f for f in fields if f not in all_fields]
        if unknown:
            return _error_text(
                f"Unknown field(s): {', '.join(unknown)}",
                f"valid fields: {', '.join(sorted(all_fields))}",
                'tg_overview fields=["name", "handle", "subscribers", "unread"]',
                "use only recognized field names",
            )
        selected_fields = list(fields) if fields else default_fields

    # Ensure client is initialized
    if _tg_client is None:
        return _error_text(
            "Telegram client not initialized",
            "server started with valid config",
            "python -m tg_mcp",
            "check ~/.tg-mcp/.env configuration",
        )

    try:
        tg = await _tg_client.get()
    except TelegramConnectionError as exc:
        return _error_text(
            str(exc),
            "connected Telegram session",
            "python -m tg_mcp.auth",
            "run auth command or check network connectivity",
        )
    except TelegramFloodWait as exc:
        return _error_text(
            f"Rate limited by Telegram. Wait {exc.seconds}s.",
            "no rate limiting",
            "retry after the wait period",
            f"wait {exc.seconds}s before retrying",
        )

    assert _settings is not None
    db = await get_db(_settings.db_path)

    # Try cache first
    channels_data: list[dict[str, Any]] | None = None
    if _cache is not None:
        channels_data = await _cache.get_channels(db)

    if channels_data is None:
        # Fetch from Telegram
        try:
            channels_data = await _fetch_all_channels(tg)
        except TelegramFloodWait as exc:
            return _error_text(
                f"Rate limited fetching channels. Wait {exc.seconds}s.",
                "no rate limiting",
                "retry after the wait period",
                f"wait {exc.seconds}s before retrying",
            )
        except Exception as exc:
            logger.exception("server.overview_fetch_error")
            return _error_text(
                f"Failed to fetch channels: {type(exc).__name__}: {exc}",
                "successful channel list fetch",
                "tg_overview",
                "check network connectivity and retry",
            )

        # Cache the result
        if _cache is not None and channels_data:
            await _cache.put_channels(db, channels_data)

    # Apply filters
    filtered = list(channels_data)

    # Type filter
    if type == "channels":
        filtered = [ch for ch in filtered if ch.get("is_channel", True)]
    elif type == "groups":
        filtered = [ch for ch in filtered if not ch.get("is_channel", True)]

    # Folder filter
    if folder is not None:
        folder_lower = folder.lower()
        filtered = [
            ch for ch in filtered
            if (ch.get("folder") or "").lower() == folder_lower
        ]

    # Min subscribers filter
    if min_subscribers > 0:
        filtered = [
            ch for ch in filtered
            if (ch.get("subscribers") or 0) >= min_subscribers
        ]

    # Sort
    filtered = _sort_channels(filtered, sort)

    # Apply limit
    total_before_limit = len(filtered)
    filtered = filtered[:limit]

    # Handle empty result
    if not filtered:
        filter_desc = []
        if folder:
            filter_desc.append(f'folder="{folder}"')
        if min_subscribers > 0:
            filter_desc.append(f"min_subscribers={min_subscribers}")
        if type != "all":
            filter_desc.append(f'type="{type}"')
        desc = " with " + ", ".join(filter_desc) if filter_desc else ""
        return toon.empty_state(
            "channels",
            f"found{desc}",
            [
                "remove or relax filters",
                "try tg_overview without filters to see all channels",
            ],
        )

    # Compute aggregates for summary
    total_channels = len(channels_data)
    total_unread = sum(ch.get("unread_count", 0) for ch in channels_data)
    folders_with_channels = len({
        ch.get("folder") for ch in channels_data if ch.get("folder")
    })
    inactive_count = sum(
        1 for ch in channels_data
        if _is_inactive(ch.get("last_post_date"), days=30)
    )

    # Build TOON rows
    toon_rows: list[list[Any]] = []
    for ch in filtered:
        row_values: list[Any] = []
        for f in selected_fields:
            if f == "name":
                row_values.append(ch.get("title", ""))
            elif f == "handle":
                handle = ch.get("handle")
                row_values.append(f"@{handle}" if handle else "")
            elif f == "subscribers":
                row_values.append(ch.get("subscribers") or 0)
            elif f == "unread":
                row_values.append(ch.get("unread_count", 0))
            elif f == "last_post":
                row_values.append(toon.format_date(ch.get("last_post_date")))
            elif f == "posts_per_week":
                row_values.append(ch.get("posts_per_week") or 0)
            elif f == "folder":
                row_values.append(ch.get("folder") or "")
            elif f == "description":
                row_values.append(ch.get("description") or "")
        toon_rows.append(row_values)

    summary_parts = [
        f"{total_channels} channels",
    ]
    if folders_with_channels > 0:
        summary_parts.append(f"{folders_with_channels} in folders")
    summary_parts.append(f"{_format_compact_number(total_unread)} total unread")
    if inactive_count > 0:
        summary_parts.append(f"{inactive_count} inactive >30d")
    if total_before_limit > limit:
        summary_parts.append(f"showing {limit} of {total_before_limit}")

    next_hints = [
        "To read messages: tg_feed channel=@handle",
    ]
    if folder is None:
        next_hints.append('To see folders: tg_overview folder="FolderName"')
    next_hints.append('To manage subscriptions: tg_search_ops query="unsubscribe"')
    next_hints.append('To manage folders: tg_search_ops query="folders"')

    return toon.format_response(
        type_name="channels",
        fields=selected_fields,
        rows=toon_rows,
        summary_parts=summary_parts,
        next_hints=next_hints,
    )


@mcp.tool(
    name="tg_search_ops",
    description=(
        "Search the operations catalog by keyword or category. "
        "Returns matching operation names with one-line descriptions. "
        "Use this to discover what Telegram operations are available before calling tg_describe_op or tg_execute. "
        "Categories: channels, messages, interact, folders, analytics."
    ),
    annotations=_READ_ONLY,
)
async def tg_search_ops(
    query: str,
    category: str | None = None,
) -> str:
    """Discover operations in the catalog."""
    if not query or not query.strip():
        return _error_text(
            "query parameter is required and cannot be empty",
            "keyword to search operations by name or description",
            'tg_search_ops query="react"',
            "provide a search keyword",
        )

    query = query.strip()

    try:
        results = catalog.search(query=query, category=category)
    except ValueError as exc:
        return _error_text(
            str(exc),
            "valid category or None",
            'tg_search_ops query="react" category="interact"',
            f"valid categories: {', '.join(sorted(catalog.VALID_CATEGORIES))}",
        )

    if not results:
        cat_note = f" in category {category!r}" if category else ""
        available_cats = catalog.list_categories()
        cat_hint = f"Available categories: {', '.join(available_cats)}" if available_cats else ""

        return (
            f'0 operations matching "{query}"{cat_note}.\n'
            f"Try: broader keywords, different spelling, or remove category filter.\n"
            f"{cat_hint}\n"
            f"\u2192 Operations are added in ops/ modules. Current total: {catalog.count()}"
        )

    lines = [f'ops[{len(results)}] matching "{query}":']
    for op in results:
        destructive_flag = " [DESTRUCTIVE]" if op.destructive else ""
        lines.append(f"  {op.name} \u2014 {op.description}{destructive_flag}")

    lines.append("")
    lines.append(f'\u2192 To see full schema: tg_describe_op name="{results[0].name}"')
    lines.append(f'\u2192 To execute: tg_execute op="{results[0].name}" params={{...}}')

    return "\n".join(lines)


@mcp.tool(
    name="tg_describe_op",
    description=(
        "Get the full schema (parameters, types, defaults, description) for a specific operation. "
        "Call tg_search_ops first to find the operation name, then this to see how to use it."
    ),
    annotations=_READ_ONLY,
)
async def tg_describe_op(name: str) -> str:
    """Get operation schema."""
    if not name or not name.strip():
        return _error_text(
            "name parameter is required",
            "operation name from tg_search_ops results",
            'tg_describe_op name="react_to_message"',
            "call tg_search_ops first to find operation names",
        )

    name = name.strip()

    try:
        return catalog.describe(name)
    except OperationError as exc:
        return exc.format()


@mcp.tool(
    name="tg_execute",
    description=(
        "Execute any operation from the catalog by name with parameters. "
        "Always call tg_describe_op first to see required parameters. "
        "For destructive operations (unsubscribe, delete), returns confirmation prompt \u2014 pass confirm=true to proceed. "
        "Rate-limited: Telegram enforces FloodWait \u2014 if hit, returns wait time."
    ),
    annotations=_EXECUTE,
)
async def tg_execute(
    op: str,
    params: dict[str, Any] | None = None,
    confirm: bool = False,
    response_format: str = "concise",
) -> str:
    """Execute a catalog operation."""
    if not op or not op.strip():
        return _error_text(
            "op parameter is required",
            "operation name from tg_search_ops results",
            'tg_execute op="react_to_message" params={"channel": "@handle", "message_id": 123, "emoji": "fire"}',
            "call tg_search_ops to find operations, then tg_describe_op for their schemas",
        )

    op = op.strip()

    valid_formats = {"concise", "detailed"}
    if response_format not in valid_formats:
        return _error_text(
            f"Invalid response_format: {response_format!r}",
            f"one of: {', '.join(sorted(valid_formats))}",
            'tg_execute op="..." response_format="concise"',
            "use 'concise' (TOON, default) or 'detailed' (full data)",
        )

    if _tg_client is None:
        return _error_text(
            "Telegram client not initialized",
            "server started with valid config",
            "python -m tg_mcp",
            "check ~/.tg-mcp/.env configuration",
        )

    try:
        tg = await _tg_client.get()
    except TelegramConnectionError as exc:
        return _error_text(
            str(exc),
            "connected Telegram session",
            "python -m tg_mcp.auth",
            "run auth command or check network connectivity",
        )
    except TelegramFloodWait as exc:
        return _error_text(
            f"Rate limited by Telegram. Waited {exc.seconds}s.",
            "no rate limiting",
            "retry after the wait period",
            f"wait {exc.seconds}s before retrying \u2014 Telegram enforces this server-side",
        )

    try:
        result = await catalog.execute(
            name=op,
            client=tg,
            cache=_cache,
            params=params,
            confirm=confirm,
        )

        if isinstance(result, str):
            return result
        return str(result) if result is not None else "Done."

    except OperationError as exc:
        return exc.format()
    except TelegramFloodWait as exc:
        return _error_text(
            f"Rate limited by Telegram during operation. Waited {exc.seconds}s.",
            "no rate limiting",
            "retry after the wait period",
            f"wait {exc.seconds}s \u2014 this is enforced by Telegram, cannot bypass",
        )
    except Exception as exc:
        logger.exception("server.execute_error", extra={"op": op})
        return _error_text(
            f"Operation {op!r} failed: {type(exc).__name__}: {exc}",
            "successful operation execution",
            f'tg_describe_op name="{op}" to verify parameters',
            "check the error message and retry with corrected parameters",
        )


# ---------------------------------------------------------------------------
# Param validation helpers
# ---------------------------------------------------------------------------


def _validate_feed_params(limit: int, hours: int) -> str | None:
    """Validate tg_feed parameters. Returns error string or None."""
    if limit < 1 or limit > 100:
        return _error_text(
            f"limit must be 1-100, got: {limit}",
            "integer between 1 and 100",
            "tg_feed limit=20",
            "use a value in the valid range",
        )
    if hours < 1 or hours > 720:
        return _error_text(
            f"hours must be 1-720 (max 30 days), got: {hours}",
            "integer between 1 and 720",
            "tg_feed hours=24",
            "use hours=168 for one week, hours=720 for 30 days",
        )
    return None


# ---------------------------------------------------------------------------
# Internal helpers — channel resolution, message fetch, sorting
# ---------------------------------------------------------------------------


async def _resolve_feed_channels(
    channel: str | None,
    folder: str | None,
) -> list[Channel | Chat]:
    """Resolve channel(s) for tg_feed. Returns list of entities.

    If channel is specified, resolves it directly.
    If channel is None, fetches all subscribed channels (optionally filtered by folder).
    """
    assert _tg_client is not None

    if channel is not None:
        return await _tg_client.resolve_channel(channel)

    # No channel specified — get all subscribed channels
    tg = await _tg_client.get()
    entities: list[Channel | Chat] = []

    try:
        async for dialog in tg.iter_dialogs():
            entity = dialog.entity
            if not isinstance(entity, (Channel, Chat)):
                continue
            if folder is not None:
                # Folder filtering: match dialog folder name
                dialog_folder = getattr(dialog, "folder", None)
                if dialog_folder is None:
                    # Telethon doesn't expose folder directly on Dialog.
                    # We compare against cached folder data if available.
                    pass
                # We'll do folder filtering post-fetch against cache
            entities.append(entity)
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e

    # Folder filter using cached channel data if available
    if folder is not None and _cache is not None and _settings is not None:
        db = await get_db(_settings.db_path)
        cached_channels = await _cache.get_channels(db)
        if cached_channels is not None:
            folder_lower = folder.lower()
            allowed_ids = {
                ch["id"] for ch in cached_channels
                if (ch.get("folder") or "").lower() == folder_lower
            }
            entities = [e for e in entities if e.id in allowed_ids]

    return entities


async def _fetch_channel_messages(
    tg: Any,
    entity: Channel | Chat,
    entity_id: int,
    handle_display: str,
    limit: int,
    cutoff_timestamp: float,
) -> list[dict[str, Any]]:
    """Fetch messages from a single channel via Telethon.

    Returns list of message dicts ready for caching and TOON formatting.
    Raises TelegramFloodWait on rate limiting.
    """
    messages: list[dict[str, Any]] = []

    try:
        async for msg in tg.iter_messages(entity, limit=limit):
            if msg is None:
                continue

            # Skip messages older than cutoff
            msg_date = msg.date
            if msg_date is not None:
                if msg_date.timestamp() < cutoff_timestamp:
                    break  # messages are in reverse chronological order

            # Extract reactions
            reactions: dict[str, int] = {}
            if msg.reactions and hasattr(msg.reactions, "results"):
                for r in msg.reactions.results:
                    emoji = getattr(r.reaction, "emoticon", None)
                    if emoji:
                        reactions[emoji] = r.count

            # Extract reply count
            replies = 0
            if msg.replies and hasattr(msg.replies, "replies"):
                replies = msg.replies.replies or 0

            # Extract forward info
            forward_from = None
            if msg.forward:
                fwd = msg.forward
                if hasattr(fwd, "chat") and fwd.chat:
                    forward_from = getattr(fwd.chat, "title", None) or str(fwd.chat.id)
                elif hasattr(fwd, "sender_id") and fwd.sender_id:
                    forward_from = str(fwd.sender_id)

            # Extract media type
            media_type = None
            if msg.media:
                media_type = type(msg.media).__name__.replace("MessageMedia", "").lower()
                if not media_type or media_type == "empty":
                    media_type = None

            # Extract author for groups (channels show channel as author)
            author = None
            if msg.post_author:
                author = msg.post_author
            elif msg.sender and hasattr(msg.sender, "first_name"):
                parts = [msg.sender.first_name or ""]
                if msg.sender.last_name:
                    parts.append(msg.sender.last_name)
                author = " ".join(parts).strip() or None

            date_str = msg_date.isoformat() if msg_date else ""

            messages.append({
                "id": msg.id,
                "channel_id": entity_id,
                "date": date_str,
                "text": msg.text or "",
                "author": author,
                "views": msg.views or 0,
                "reactions": reactions,
                "replies": replies,
                "forward_from": forward_from,
                "media_type": media_type,
                "_channel_title": handle_display,
            })

    except FloodWaitError as e:
        logger.warning(
            "server.flood_wait_fetching_messages",
            extra={"channel": handle_display, "wait_seconds": e.seconds},
        )
        raise TelegramFloodWait(e.seconds) from e

    return messages


async def _fetch_all_channels(tg: Any) -> list[dict[str, Any]]:
    """Fetch all subscribed channels/groups from Telegram.

    Returns list of channel dicts ready for caching.
    Raises TelegramFloodWait on rate limiting.
    """
    channels: list[dict[str, Any]] = []

    try:
        async for dialog in tg.iter_dialogs():
            entity = dialog.entity
            if not isinstance(entity, (Channel, Chat)):
                continue

            is_channel = isinstance(entity, Channel) and entity.broadcast
            handle = getattr(entity, "username", None)

            # Compute posts_per_week from dialog metadata (rough estimate)
            # We don't have historical data in a single API call, so this is a
            # placeholder that will be refined when channel_stats is called.
            posts_per_week: float | None = None

            last_post_date: str | None = None
            if dialog.date:
                last_post_date = dialog.date.isoformat()

            # Subscriber count: requires full channel request for accuracy.
            # For the overview, we use what's available locally (may be None).
            subscribers = getattr(entity, "participants_count", None)

            channels.append({
                "id": entity.id,
                "title": dialog.name or getattr(entity, "title", ""),
                "handle": handle,
                "subscribers": subscribers,
                "is_channel": is_channel,
                "folder": None,  # Populated via folder ops
                "last_post_date": last_post_date,
                "posts_per_week": posts_per_week,
                "unread_count": dialog.unread_count or 0,
            })

    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e

    return channels


def _sort_channels(
    channels: list[dict[str, Any]], sort: str
) -> list[dict[str, Any]]:
    """Sort channel list by the given sort key."""
    if sort == "unread":
        return sorted(channels, key=lambda c: c.get("unread_count", 0), reverse=True)
    elif sort == "activity":
        return sorted(
            channels,
            key=lambda c: c.get("posts_per_week") or 0,
            reverse=True,
        )
    elif sort == "subscribers":
        return sorted(
            channels,
            key=lambda c: c.get("subscribers") or 0,
            reverse=True,
        )
    elif sort == "name":
        return sorted(channels, key=lambda c: (c.get("title") or "").lower())
    elif sort == "last_post":
        return sorted(
            channels,
            key=lambda c: c.get("last_post_date") or "",
            reverse=True,
        )
    return channels


def _msg_timestamp(msg: dict[str, Any]) -> float:
    """Extract a comparable timestamp from a message dict.

    Returns 0.0 if date is missing or unparseable (sorts to bottom).
    """
    date_val = msg.get("date")
    if not date_val:
        return 0.0

    if isinstance(date_val, (int, float)):
        return float(date_val)

    if isinstance(date_val, str):
        try:
            dt = datetime.fromisoformat(date_val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except (ValueError, TypeError):
            return 0.0

    if isinstance(date_val, datetime):
        return date_val.timestamp()

    return 0.0


def _is_inactive(last_post_date: str | None, days: int) -> bool:
    """Check if a channel's last post is older than N days."""
    if not last_post_date:
        return True

    try:
        dt = datetime.fromisoformat(last_post_date)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - dt).total_seconds()
        return age_seconds > days * 86400
    except (ValueError, TypeError):
        return True


def _format_compact_number(n: int | float) -> str:
    """Format a number compactly: 1234 -> '1.2K', 1234567 -> '1.2M'."""
    n = int(n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------


async def run_server() -> None:
    """Initialize and run the MCP server with stdio transport."""
    global _settings, _tg_client, _cache

    # Load config — fail fast if misconfigured
    try:
        _settings = load_settings()
    except ConfigError as exc:
        logger.error("server.config_error", extra={"error": str(exc)})
        raise SystemExit(f"Configuration error:\n{exc}") from exc

    # Create Telegram client wrapper (lazy — no connection yet)
    _tg_client = TelegramClient(_settings)

    # Create cache instance
    _cache = Cache()

    # Import ops to trigger @operation() registration
    import tg_mcp.ops  # noqa: F401

    logger.info(
        "server.starting",
        extra={
            "tools": 5,
            "operations": catalog.count(),
            "categories": catalog.list_categories(),
        },
    )

    try:
        await mcp.run_stdio_async()
    finally:
        if _tg_client is not None:
            await _tg_client.disconnect()

        from tg_mcp.db import close_db
        await close_db()

        logger.info("server.stopped")
