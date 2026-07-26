"""Message operations — search, read, history, analytics.

Read-only operations for finding and inspecting messages across channels.
Registered into the catalog via @operation() decorator.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
)
from telethon.tl.types import Channel, Chat, User

from tg_mcp import toon
from tg_mcp.cache import Cache
from tg_mcp.catalog import OperationError, operation
from tg_mcp.client import TelegramFloodWait
from tg_mcp.config import logger
from tg_mcp.ops.channels import _resolve_single_channel


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_media_info(msg: Any) -> str:
    """Extract a human-readable media type string from a message.

    Returns empty string if no media is present.
    """
    media = getattr(msg, "media", None)
    if media is None:
        return ""

    type_name = type(media).__name__
    # Strip "MessageMedia" prefix for readability: MessageMediaPhoto -> Photo
    if type_name.startswith("MessageMedia"):
        type_name = type_name[len("MessageMedia"):]
    return type_name.lower() or "unknown"


def _truncate_text(text: str | None, max_len: int = 200) -> str:
    """Truncate text to max_len characters, appending ellipsis if cut."""
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def _channel_display(msg: Any) -> str:
    """Extract a display name for the channel/chat/user a message belongs to."""
    chat = getattr(msg, "chat", None)
    if chat is None:
        return ""
    handle = getattr(chat, "username", None)
    if handle:
        return f"@{handle}"
    if isinstance(chat, User):
        first = getattr(chat, "first_name", "") or ""
        last = getattr(chat, "last_name", "") or ""
        return f"{first} {last}".strip() or ""
    return getattr(chat, "title", "") or ""


_DEFAULT_DOWNLOAD_DIR = "~/Downloads/Telegram Desktop"


def _resolve_download_dir(dest_dir: str) -> Path:
    """Resolve the download destination, creating it if missing.

    Precedence: explicit dest_dir > TG_MCP_DOWNLOAD_DIR > ~/Downloads/Telegram Desktop.
    """
    raw = (dest_dir or "").strip()
    if not raw:
        raw = (os.environ.get("TG_MCP_DOWNLOAD_DIR") or "").strip()
    if not raw:
        raw = _DEFAULT_DOWNLOAD_DIR

    path = Path(os.path.expandvars(os.path.expanduser(raw)))

    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise OperationError(
            what=f"Cannot create download directory {path}: {exc}",
            expected="a writable directory path",
            example='params={"channel": "@handle", "message_id": 1, "dest_dir": "~/Downloads"}',
            recovery="pass a writable dest_dir or set TG_MCP_DOWNLOAD_DIR",
        ) from exc

    if not path.is_dir():
        raise OperationError(
            what=f"Download destination is not a directory: {path}",
            expected="a directory path",
            example='params={"channel": "@handle", "message_id": 1, "dest_dir": "~/Downloads"}',
            recovery="pass a directory in dest_dir or set TG_MCP_DOWNLOAD_DIR",
        )

    return path


def _safe_filename(name: str, fallback: str) -> str:
    """Strip any path components from a filename, falling back if unusable."""
    cleaned = Path((name or "").strip().replace("\\", "/")).name.strip()
    if not cleaned or cleaned in (".", ".."):
        return fallback
    return cleaned


def _unique_path(directory: Path, name: str, size: int | None) -> tuple[Path, bool]:
    """Pick a non-clobbering target path, Telegram Desktop style: "name (1).ext".

    Returns (path, already_downloaded). If an existing file has the same size it
    is treated as the same media and reused instead of re-downloading.
    """
    stem, ext = os.path.splitext(name)
    candidate = directory / name
    counter = 0

    while candidate.exists():
        if size is not None and candidate.is_file() and candidate.stat().st_size == size:
            return candidate, True
        counter += 1
        candidate = directory / f"{stem} ({counter}){ext}"

    return candidate, False


def _parse_date_filter(date_str: str | None, param_name: str) -> datetime | None:
    """Parse an ISO 8601 date string into a timezone-aware datetime.

    Accepts YYYY-MM-DD or full ISO 8601. Returns None if date_str is None/empty.
    Raises OperationError on invalid format.
    """
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()
    # Try YYYY-MM-DD first (most common)
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    raise OperationError(
        what=f"Invalid date format for {param_name}: {date_str!r}",
        expected="ISO 8601 date: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS",
        example=f'params={{"{param_name}": "2026-04-01"}}',
        recovery="use YYYY-MM-DD format",
    )


# ---------------------------------------------------------------------------
# search_messages (T021)
# ---------------------------------------------------------------------------


@operation(
    name="search_messages",
    category="messages",
    description="Search messages by keyword across all channels or within a specific channel. Returns matching messages with channel attribution",
    destructive=False,
    idempotent=True,
)
async def search_messages(
    client: Any,
    query: str,
    channel: str = "",
    limit: int = 20,
    after: str = "",
    before: str = "",
    cache: Cache | None = None,
) -> str:
    """Search messages by keyword across channels."""
    # --- Input validation ---
    if not query or not query.strip():
        raise OperationError(
            what="query parameter is required and cannot be empty",
            expected="non-empty search keyword or phrase",
            example='tg_execute op="search_messages" params={"query": "LLM benchmark"}',
            recovery="provide a search term",
        )

    query = query.strip()

    if limit < 1 or limit > 100:
        raise OperationError(
            what=f"limit must be 1-100, got: {limit}",
            expected="integer between 1 and 100",
            example='tg_execute op="search_messages" params={"query": "test", "limit": 20}',
            recovery="use a value in the valid range",
        )

    after_dt = _parse_date_filter(after or None, "after")
    before_dt = _parse_date_filter(before or None, "before")

    if after_dt and before_dt and after_dt >= before_dt:
        raise OperationError(
            what=f"'after' ({after}) must be earlier than 'before' ({before})",
            expected="after < before date range",
            example='params={"query": "test", "after": "2026-03-01", "before": "2026-04-01"}',
            recovery="swap the dates or adjust the range",
        )

    # --- Resolve entity (None = global search) ---
    entity: Any = None
    if channel and channel.strip():
        entity = await _resolve_single_channel(client, channel.strip())

    # --- Execute search ---
    iter_kwargs: dict[str, Any] = {
        "search": query,
        "limit": limit,
    }
    if after_dt:
        # Telethon's offset_date filters messages BEFORE this date,
        # so we use it for 'before'. For 'after', we filter client-side.
        pass
    if before_dt:
        iter_kwargs["offset_date"] = before_dt

    results: list[dict[str, Any]] = []

    try:
        async for msg in client.iter_messages(entity, **iter_kwargs):
            if msg is None:
                continue

            # Client-side 'after' filter: stop when messages are older than after_dt
            if after_dt and msg.date and msg.date.replace(tzinfo=timezone.utc) < after_dt:
                break

            text_preview = _truncate_text(msg.text)
            media = _extract_media_info(msg)

            results.append({
                "channel": _channel_display(msg),
                "id": msg.id,
                "date": toon.format_date(msg.date) if msg.date else "",
                "text": text_preview or (f"[{media}]" if media else "[no text]"),
                "views": msg.views or 0,
            })
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except ChannelPrivateError:
        raise OperationError(
            what=f"Channel {channel} is private or you were banned",
            expected="accessible channel for search",
            example='tg_execute op="search_messages" params={"query": "test"}',
            recovery="remove the channel filter to search globally, or use a channel you have access to",
        )
    except Exception as exc:
        logger.exception("ops.search_messages_error", extra={"query": query})
        raise OperationError(
            what=f"Search failed: {type(exc).__name__}: {exc}",
            expected="successful message search",
            example='tg_execute op="search_messages" params={"query": "test"}',
            recovery="simplify the query and retry",
        ) from exc

    # --- Format response ---
    if not results:
        scope = f"in {channel}" if channel else "globally"
        date_hint = ""
        if after or before:
            date_hint = f" (date range: {after or '...'} to {before or '...'})"
        return toon.empty_state(
            "messages",
            f"matching {query!r} {scope}{date_hint}",
            [
                "try broader keywords",
                "remove date filters",
                "search globally (omit channel)",
            ],
        )

    fields = ["channel", "id", "date", "text", "views"]
    rows = [
        [r["channel"], r["id"], r["date"], r["text"], r["views"]]
        for r in results
    ]

    summary_parts = [f"{len(results)} results"]
    if len(results) == limit:
        summary_parts.append(f"limit reached — increase limit for more")

    scope_desc = f"in {channel}" if channel else "global"
    summary_parts.append(scope_desc)

    return toon.format_response(
        type_name="messages",
        fields=fields,
        rows=rows,
        summary_parts=summary_parts,
        next_hints=[
            'Full message: tg_execute op="get_message" params={"channel": "<channel>", "message_id": <id>}',
            'React: tg_execute op="react_to_message" params={"channel": "<channel>", "message_id": <id>, "emoji": "\U0001f44d"}',
        ],
    )


# ---------------------------------------------------------------------------
# get_message (T022)
# ---------------------------------------------------------------------------


@operation(
    name="get_message",
    category="messages",
    description="Get a single message by channel and message ID. Returns full content, media metadata, reactions, and reply info",
    destructive=False,
    idempotent=True,
)
async def get_message(
    client: Any,
    channel: str,
    message_id: int,
    cache: Cache | None = None,
) -> str:
    """Get a single message with full details."""
    # --- Input validation ---
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="get_message" params={"channel": "@llm_under_hood", "message_id": 123}',
            recovery="provide a channel identifier",
        )

    if message_id < 1:
        raise OperationError(
            what=f"message_id must be a positive integer, got: {message_id}",
            expected="valid Telegram message ID (positive integer)",
            example='tg_execute op="get_message" params={"channel": "@handle", "message_id": 123}',
            recovery="use a valid message ID from search or feed results",
        )

    channel = channel.strip()
    entity = await _resolve_single_channel(client, channel)

    # --- Fetch message ---
    try:
        msgs = await client.get_messages(entity, ids=message_id)
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except ChannelPrivateError:
        raise OperationError(
            what=f"Channel {channel} is private or you were banned",
            expected="accessible channel",
            example='tg_execute op="get_message" params={"channel": "@public_channel", "message_id": 1}',
            recovery="you need to be a member to access this channel",
        )
    except Exception as exc:
        logger.exception(
            "ops.get_message_error",
            extra={"channel": channel, "message_id": message_id},
        )
        raise OperationError(
            what=f"Failed to fetch message {message_id} from {channel}: {type(exc).__name__}: {exc}",
            expected="successful message fetch",
            example=f'tg_execute op="get_message" params={{"channel": "{channel}", "message_id": {message_id}}}',
            recovery="check channel access and message ID, then retry",
        ) from exc

    # get_messages with ids= returns a single Message or None
    msg = msgs
    if isinstance(msgs, list):
        msg = msgs[0] if msgs else None

    if msg is None:
        raise OperationError(
            what=f"Message {message_id} not found in {channel}",
            expected="existing message ID",
            example=f'tg_execute op="search_messages" params={{"channel": "{channel}", "query": "keyword"}}',
            recovery="the message may have been deleted — search for it by content",
        )

    # --- Build response ---
    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)

    lines = [
        f"channel: {handle_display}",
        f"message_id: {msg.id}",
        f"date: {toon.format_date(msg.date) if msg.date else 'unknown'}",
    ]

    # Sender info
    sender = getattr(msg, "sender", None)
    if sender:
        sender_name = getattr(sender, "first_name", "") or ""
        sender_last = getattr(sender, "last_name", "") or ""
        sender_username = getattr(sender, "username", None)
        sender_display = f"{sender_name} {sender_last}".strip()
        if sender_username:
            sender_display += f" (@{sender_username})"
        if sender_display:
            lines.append(f"sender: {sender_display}")

    # Content
    if msg.text:
        lines.append(f"text: {msg.text}")
    else:
        lines.append("text: (no text)")

    # Media
    media_type = _extract_media_info(msg)
    if media_type:
        lines.append(f"media: {media_type}")

    # Views, forwards
    if msg.views is not None:
        lines.append(f"views: {msg.views}")
    if msg.forwards is not None:
        lines.append(f"forwards: {msg.forwards}")

    # Reactions
    if msg.reactions and hasattr(msg.reactions, "results"):
        reaction_parts = []
        for r in msg.reactions.results:
            emoticon = getattr(r.reaction, "emoticon", None) or "?"
            reaction_parts.append(f"{emoticon} {r.count}")
        if reaction_parts:
            lines.append(f"reactions: {', '.join(reaction_parts)}")

    # Replies
    if msg.replies and hasattr(msg.replies, "replies"):
        lines.append(f"replies: {msg.replies.replies or 0}")

    # Reply-to
    if msg.reply_to and hasattr(msg.reply_to, "reply_to_msg_id"):
        lines.append(f"reply_to: {msg.reply_to.reply_to_msg_id}")

    # Hints
    lines.append("")
    lines.append(
        toon.hint(
            f'React: tg_execute op="react_to_message" '
            f'params={{"channel": "{handle_display}", "message_id": {msg.id}, "emoji": "\U0001f44d"}}'
        )
    )
    lines.append(
        toon.hint(
            f'Forward: tg_execute op="forward_message" '
            f'params={{"channel": "{handle_display}", "message_id": {msg.id}}}'
        )
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# download_media
# ---------------------------------------------------------------------------


@operation(
    name="download_media",
    category="messages",
    description="Download the file attached to a message (document, photo, video, audio) to local disk. Keeps the original filename and never overwrites — writes 'name (1).ext' on collision. Defaults to ~/Downloads/Telegram Desktop",
    destructive=False,
    idempotent=True,
)
async def download_media(
    client: Any,
    channel: str,
    message_id: int,
    filename: str = "",
    dest_dir: str = "",
    cache: Cache | None = None,
) -> str:
    """Download a message's media attachment to the local filesystem."""
    # --- Input validation ---
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="download_media" params={"channel": "@osstya", "message_id": 123}',
            recovery="provide a channel identifier",
        )

    if message_id < 1:
        raise OperationError(
            what=f"message_id must be a positive integer, got: {message_id}",
            expected="valid Telegram message ID (positive integer)",
            example='tg_execute op="download_media" params={"channel": "@handle", "message_id": 123}',
            recovery="use a valid message ID from search or feed results",
        )

    channel = channel.strip()
    entity = await _resolve_single_channel(client, channel)
    target_dir = _resolve_download_dir(dest_dir)

    # --- Fetch message ---
    try:
        msgs = await client.get_messages(entity, ids=message_id)
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except ChannelPrivateError:
        raise OperationError(
            what=f"Channel {channel} is private or you were banned",
            expected="accessible channel",
            example='tg_execute op="download_media" params={"channel": "@public_channel", "message_id": 1}',
            recovery="you need to be a member to access this channel",
        )
    except Exception as exc:
        logger.exception(
            "ops.download_media_error",
            extra={"channel": channel, "message_id": message_id},
        )
        raise OperationError(
            what=f"Failed to fetch message {message_id} from {channel}: {type(exc).__name__}: {exc}",
            expected="successful message fetch",
            example=f'tg_execute op="download_media" params={{"channel": "{channel}", "message_id": {message_id}}}',
            recovery="check channel access and message ID, then retry",
        ) from exc

    msg = msgs
    if isinstance(msgs, list):
        msg = msgs[0] if msgs else None

    if msg is None:
        raise OperationError(
            what=f"Message {message_id} not found in {channel}",
            expected="existing message ID",
            example=f'tg_execute op="search_messages" params={{"channel": "{channel}", "query": "keyword"}}',
            recovery="the message may have been deleted — search for it by content",
        )

    media_file = getattr(msg, "file", None)
    if getattr(msg, "media", None) is None or media_file is None:
        raise OperationError(
            what=f"Message {message_id} in {channel} has no downloadable media",
            expected="a message carrying a document, photo, video, or audio file",
            example=f'tg_execute op="get_message" params={{"channel": "{channel}", "message_id": {message_id}}}',
            recovery="inspect the message with get_message to see whether it has media",
        )

    # --- Resolve target filename ---
    mime = getattr(media_file, "mime_type", None) or ""
    expected_size = getattr(media_file, "size", None)
    ext = getattr(media_file, "ext", None) or ""
    original_name = getattr(media_file, "name", None) or ""
    fallback_name = f"{mime.split('/')[0] or 'file'}_{msg.id}{ext}"

    if filename and filename.strip():
        out_name = _safe_filename(filename, fallback_name)
    else:
        out_name = _safe_filename(original_name, fallback_name)

    target, already_downloaded = _unique_path(target_dir, out_name, expected_size)

    # --- Download ---
    if not already_downloaded:
        try:
            written = await client.download_media(msg, file=str(target))
        except FloodWaitError as e:
            raise TelegramFloodWait(e.seconds) from e
        except Exception as exc:
            logger.exception(
                "ops.download_media_error",
                extra={"channel": channel, "message_id": message_id},
            )
            raise OperationError(
                what=f"Download of message {message_id} from {channel} failed: {type(exc).__name__}: {exc}",
                expected="media written to disk",
                example=f'tg_execute op="download_media" params={{"channel": "{channel}", "message_id": {message_id}}}',
                recovery="check free disk space and directory permissions, then retry",
            ) from exc

        if not written:
            raise OperationError(
                what=f"Telegram returned no file for message {message_id} in {channel}",
                expected="media written to disk",
                example=f'tg_execute op="get_message" params={{"channel": "{channel}", "message_id": {message_id}}}',
                recovery="the media may be unsupported (e.g. a web page preview) — inspect it with get_message",
            )

        target = Path(written)

    # --- Build response ---
    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)
    size_bytes = target.stat().st_size

    lines = [
        f"path: {target}",
        f"filename: {target.name}",
        f"size_bytes: {size_bytes}",
        f"mime_type: {mime or 'unknown'}",
        f"channel: {handle_display}",
        f"message_id: {msg.id}",
    ]
    if already_downloaded:
        lines.append("status: already downloaded — existing file reused")
    if original_name and target.name != original_name:
        lines.append(f"original_filename: {original_name}")

    lines.append("")
    lines.append(
        toon.hint(
            f'Message details: tg_execute op="get_message" '
            f'params={{"channel": "{handle_display}", "message_id": {msg.id}}}'
        )
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# message_history (T023)
# ---------------------------------------------------------------------------


@operation(
    name="message_history",
    category="messages",
    description="Get paginated message history for a channel. Returns messages in reverse chronological order with pagination support",
    destructive=False,
    idempotent=True,
)
async def message_history(
    client: Any,
    channel: str,
    limit: int = 20,
    before_id: int = 0,
    cache: Cache | None = None,
) -> str:
    """Get paginated message history for a channel."""
    # --- Input validation ---
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="message_history" params={"channel": "@llm_under_hood"}',
            recovery="provide a channel identifier",
        )

    if limit < 1 or limit > 100:
        raise OperationError(
            what=f"limit must be 1-100, got: {limit}",
            expected="integer between 1 and 100",
            example='tg_execute op="message_history" params={"channel": "@handle", "limit": 20}',
            recovery="use a value in the valid range",
        )

    if before_id < 0:
        raise OperationError(
            what=f"before_id must be non-negative, got: {before_id}",
            expected="message ID to paginate from, or 0 for latest",
            example='tg_execute op="message_history" params={"channel": "@handle", "before_id": 0}',
            recovery="use 0 for latest messages or a valid message ID from prior results",
        )

    channel = channel.strip()
    entity = await _resolve_single_channel(client, channel)
    is_dm = isinstance(entity, User)

    # --- Fetch messages ---
    iter_kwargs: dict[str, Any] = {"limit": limit}
    if before_id > 0:
        iter_kwargs["max_id"] = before_id

    messages: list[dict[str, Any]] = []
    last_id: int = 0

    try:
        async for msg in client.iter_messages(entity, **iter_kwargs):
            if msg is None:
                continue

            text_preview = _truncate_text(msg.text)
            media = _extract_media_info(msg)

            row: dict[str, Any] = {
                "id": msg.id,
                "date": toon.format_date(msg.date) if msg.date else "",
                "text": text_preview or (f"[{media}]" if media else "[no text]"),
                "views": msg.views or 0,
                "reactions": _count_reactions(msg),
            }

            if is_dm:
                sender = getattr(msg, "sender", None)
                if sender is not None:
                    first = getattr(sender, "first_name", "") or ""
                    last = getattr(sender, "last_name", "") or ""
                    row["sender"] = f"{first} {last}".strip() or "you"
                else:
                    row["sender"] = "you" if msg.out else "them"

            messages.append(row)
            last_id = msg.id
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except ChannelPrivateError:
        raise OperationError(
            what=f"Channel {channel} is private or you were banned",
            expected="accessible channel",
            example='tg_execute op="message_history" params={"channel": "@public_channel"}',
            recovery="you need to be a member to access this channel",
        )
    except Exception as exc:
        logger.exception("ops.message_history_error", extra={"channel": channel})
        raise OperationError(
            what=f"Failed to fetch history for {channel}: {type(exc).__name__}: {exc}",
            expected="successful message fetch",
            example=f'tg_execute op="message_history" params={{"channel": "{channel}"}}',
            recovery="check channel access and retry",
        ) from exc

    handle = getattr(entity, "username", None)
    if is_dm:
        first = getattr(entity, "first_name", "") or ""
        last = getattr(entity, "last_name", "") or ""
        handle_display = f"@{handle}" if handle else f"{first} {last}".strip() or channel
    else:
        handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)

    if not messages:
        return toon.empty_state(
            "messages",
            f"in {handle_display}" + (f" before ID {before_id}" if before_id else ""),
            ["chat may be empty", "check before_id value"],
        )

    if is_dm:
        fields = ["id", "date", "sender", "text", "reactions"]
        rows = [
            [m["id"], m["date"], m.get("sender", ""), m["text"], m["reactions"]]
            for m in messages
        ]
    else:
        fields = ["id", "date", "text", "views", "reactions"]
        rows = [
            [m["id"], m["date"], m["text"], m["views"], m["reactions"]]
            for m in messages
        ]

    summary_parts = [f"{len(messages)} messages", handle_display]

    # Pagination hint: if we got a full page, there are likely more
    next_hints = [
        f'Full message: tg_execute op="get_message" params={{"channel": "{handle_display}", "message_id": <id>}}',
    ]
    if len(messages) == limit and last_id > 0:
        next_hints.append(
            f'Next page: tg_execute op="message_history" params={{"channel": "{handle_display}", "limit": {limit}, "before_id": {last_id}}}'
        )

    return toon.format_response(
        type_name="messages",
        fields=fields,
        rows=rows,
        summary_parts=summary_parts,
        next_hints=next_hints,
    )


# ---------------------------------------------------------------------------
# saved_messages
# ---------------------------------------------------------------------------


@operation(
    name="saved_messages",
    category="messages",
    description="Get messages from your personal Saved Messages chat (the 'me' self-chat). Supports pagination and full-text mode for cleanup workflows",
    destructive=False,
    idempotent=True,
)
async def saved_messages(
    client: Any,
    limit: int = 50,
    before_id: int = 0,
    include_full_text: bool = False,
    cache: Cache | None = None,
) -> str:
    """Read messages from Saved Messages (self-chat)."""
    if limit < 1 or limit > 300:
        raise OperationError(
            what=f"limit must be 1-300, got: {limit}",
            expected="integer between 1 and 300",
            example='tg_execute op="saved_messages" params={"limit": 100}',
            recovery="use a value in the valid range",
        )

    if before_id < 0:
        raise OperationError(
            what=f"before_id must be non-negative, got: {before_id}",
            expected="message ID to paginate from, or 0 for latest",
            example='tg_execute op="saved_messages" params={"before_id": 0}',
            recovery="use 0 for latest messages or a valid message ID from prior results",
        )

    iter_kwargs: dict[str, Any] = {"limit": limit}
    if before_id > 0:
        iter_kwargs["max_id"] = before_id

    messages: list[dict[str, Any]] = []
    last_id: int = 0

    try:
        async for msg in client.iter_messages("me", **iter_kwargs):
            if msg is None:
                continue

            text = msg.text or ""
            if not include_full_text:
                text = _truncate_text(text, max_len=200)
            text = text.replace("\n", " ⏎ ")

            media = _extract_media_info(msg)

            # Identify forward source
            kind = "own"
            source = ""
            fwd = getattr(msg, "forward", None)
            if fwd:
                kind = "fwd"
                fwd_chat = getattr(fwd, "chat", None)
                fwd_sender = getattr(fwd, "sender", None)
                if fwd_chat is not None:
                    handle = getattr(fwd_chat, "username", None)
                    source = f"@{handle}" if handle else (getattr(fwd_chat, "title", "") or "")
                elif fwd_sender is not None:
                    handle = getattr(fwd_sender, "username", None)
                    if handle:
                        source = f"@{handle}"
                    else:
                        first = getattr(fwd_sender, "first_name", "") or ""
                        last = getattr(fwd_sender, "last_name", "") or ""
                        source = f"{first} {last}".strip() or "?"
                else:
                    from_name = getattr(fwd, "from_name", None)
                    source = from_name or "?"

            messages.append({
                "id": msg.id,
                "date": toon.format_date(msg.date) if msg.date else "",
                "kind": kind,
                "source": source,
                "media": media,
                "text": text or (f"[{media}]" if media else "[no text]"),
            })
            last_id = msg.id
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.saved_messages_error")
        raise OperationError(
            what=f"Failed to fetch Saved Messages: {type(exc).__name__}: {exc}",
            expected="successful read of self-chat history",
            example='tg_execute op="saved_messages" params={"limit": 100}',
            recovery="retry; if FloodWait persists, lower limit",
        ) from exc

    if not messages:
        return toon.empty_state(
            "saved_messages",
            "in Saved Messages" + (f" before ID {before_id}" if before_id else ""),
            ["self-chat is empty", "or before_id is older than first message"],
        )

    fields = ["id", "date", "kind", "source", "media", "text"]
    rows = [
        [m["id"], m["date"], m["kind"], m["source"], m["media"], m["text"]]
        for m in messages
    ]

    summary_parts = [f"{len(messages)} messages", "Saved Messages"]
    if include_full_text:
        summary_parts.append("full text")

    next_hints = [
        'Delete: tg_execute op="delete_message" params={"channel": "me", "message_id": <id>}',
    ]
    if len(messages) == limit and last_id > 0:
        next_hints.append(
            f'Next page: tg_execute op="saved_messages" params={{"limit": {limit}, "before_id": {last_id}}}'
        )

    return toon.format_response(
        type_name="saved_messages",
        fields=fields,
        rows=rows,
        summary_parts=summary_parts,
        next_hints=next_hints,
    )


# ---------------------------------------------------------------------------
# who_posted_first (T024)
# ---------------------------------------------------------------------------


@operation(
    name="who_posted_first",
    category="messages",
    description="Search for a keyword and find which channel posted about it first. Groups results by channel sorted by earliest mention",
    destructive=False,
    idempotent=True,
)
async def who_posted_first(
    client: Any,
    query: str,
    limit: int = 50,
    cache: Cache | None = None,
) -> str:
    """Search keyword globally, group by channel, sort by earliest post."""
    # --- Input validation ---
    if not query or not query.strip():
        raise OperationError(
            what="query parameter is required and cannot be empty",
            expected="non-empty search keyword or phrase",
            example='tg_execute op="who_posted_first" params={"query": "GPT-5"}',
            recovery="provide a search term",
        )

    query = query.strip()

    if limit < 1 or limit > 100:
        raise OperationError(
            what=f"limit must be 1-100, got: {limit}",
            expected="integer between 1 and 100 (number of messages to scan)",
            example='tg_execute op="who_posted_first" params={"query": "GPT-5", "limit": 50}',
            recovery="use a value in the valid range",
        )

    # --- Global search ---
    # channel_id -> {channel_display, earliest_date, earliest_msg_id, count}
    channel_map: dict[int, dict[str, Any]] = {}

    try:
        async for msg in client.iter_messages(None, search=query, limit=limit):
            if msg is None:
                continue

            chat = getattr(msg, "chat", None)
            if chat is None or not isinstance(chat, (Channel, Chat, User)):
                continue

            chat_id = chat.id
            display = _channel_display(msg)
            msg_date = msg.date

            if chat_id not in channel_map:
                channel_map[chat_id] = {
                    "channel": display,
                    "earliest_date": msg_date,
                    "earliest_id": msg.id,
                    "count": 1,
                }
            else:
                entry = channel_map[chat_id]
                entry["count"] += 1
                # Keep track of the earliest (oldest) message
                if msg_date and entry["earliest_date"] and msg_date < entry["earliest_date"]:
                    entry["earliest_date"] = msg_date
                    entry["earliest_id"] = msg.id
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.who_posted_first_error", extra={"query": query})
        raise OperationError(
            what=f"Global search failed: {type(exc).__name__}: {exc}",
            expected="successful global message search",
            example='tg_execute op="who_posted_first" params={"query": "keyword"}',
            recovery="simplify the query and retry",
        ) from exc

    if not channel_map:
        return toon.empty_state(
            "channels",
            f"mentioning {query!r}",
            [
                "try broader keywords",
                "check spelling",
                f'or search directly: tg_execute op="search_messages" params={{"query": "{query}"}}',
            ],
        )

    # Sort by earliest date (oldest first = posted first)
    sorted_channels = sorted(
        channel_map.values(),
        key=lambda c: c["earliest_date"] or datetime.max.replace(tzinfo=timezone.utc),
    )

    fields = ["rank", "channel", "first_posted", "msg_id", "mentions"]
    rows = []
    for rank, ch in enumerate(sorted_channels, 1):
        rows.append([
            rank,
            ch["channel"],
            toon.format_date(ch["earliest_date"]) if ch["earliest_date"] else "",
            ch["earliest_id"],
            ch["count"],
        ])

    summary_parts = [
        f"{len(sorted_channels)} channels",
        f"scanned {limit} messages",
        f"query: {query!r}",
    ]

    winner = sorted_channels[0]

    return toon.format_response(
        type_name="first_posters",
        fields=fields,
        rows=rows,
        summary_parts=summary_parts,
        next_hints=[
            f'First post: tg_execute op="get_message" params={{"channel": "{winner["channel"]}", "message_id": {winner["earliest_id"]}}}',
            f'More results: tg_execute op="who_posted_first" params={{"query": "{query}", "limit": 100}}',
        ],
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _count_reactions(msg: Any) -> int:
    """Sum all reaction counts on a message. Returns 0 if none."""
    if not msg.reactions or not hasattr(msg.reactions, "results"):
        return 0
    total = 0
    for r in msg.reactions.results:
        total += r.count
    return total
