"""Channel operations — list, info, stats.

Read-only operations for inspecting subscribed channels.
Registered into the catalog via @operation() decorator.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import Channel, Chat

from tg_mcp import toon
from tg_mcp.cache import Cache, CacheCategory, make_cache_key
from tg_mcp.catalog import OperationError, operation
from tg_mcp.client import ChannelResolutionError, TelegramFloodWait
from tg_mcp.config import logger


# ---------------------------------------------------------------------------
# list_channels
# ---------------------------------------------------------------------------


@operation(
    name="list_channels",
    category="channels",
    description="List all subscribed channels and groups with basic info: title, handle, subscriber count, unread count",
    destructive=False,
    idempotent=True,
)
async def list_channels(
    client: Any,
    cache: Cache | None = None,
    type: str = "all",
    limit: int = 100,
    sort: str = "name",
) -> str:
    """List all subscribed channels with basic info."""
    # Validate type
    valid_types = {"channels", "groups", "all"}
    if type not in valid_types:
        raise OperationError(
            what=f"Invalid type filter: {type!r}",
            expected=f"one of: {', '.join(sorted(valid_types))}",
            example='tg_execute op="list_channels" params={"type": "channels"}',
            recovery="use 'channels', 'groups', or 'all'",
        )

    # Validate sort
    valid_sorts = {"name", "unread", "subscribers", "last_post"}
    if sort not in valid_sorts:
        raise OperationError(
            what=f"Invalid sort: {sort!r}",
            expected=f"one of: {', '.join(sorted(valid_sorts))}",
            example='tg_execute op="list_channels" params={"sort": "name"}',
            recovery="use one of the listed sort options",
        )

    # Validate limit
    if limit < 1 or limit > 500:
        raise OperationError(
            what=f"limit must be 1-500, got: {limit}",
            expected="integer between 1 and 500",
            example='tg_execute op="list_channels" params={"limit": 50}',
            recovery="use a value in the valid range",
        )

    channels: list[dict[str, Any]] = []

    try:
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if not isinstance(entity, (Channel, Chat)):
                continue

            is_channel = isinstance(entity, Channel) and entity.broadcast

            # Apply type filter early to avoid unnecessary processing
            if type == "channels" and not is_channel:
                continue
            if type == "groups" and is_channel:
                continue

            handle = getattr(entity, "username", None)
            subscribers = getattr(entity, "participants_count", None)

            channels.append({
                "title": dialog.name or getattr(entity, "title", ""),
                "handle": f"@{handle}" if handle else "",
                "subscribers": subscribers or 0,
                "unread": dialog.unread_count or 0,
                "last_post": dialog.date.isoformat() if dialog.date else "",
                "type": "channel" if is_channel else "group",
            })
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e

    if not channels:
        return toon.empty_state(
            "channels",
            f"found (type={type})",
            ["try type='all'", "check Telegram subscriptions"],
        )

    # Sort
    if sort == "name":
        channels.sort(key=lambda c: c["title"].lower())
    elif sort == "unread":
        channels.sort(key=lambda c: c["unread"], reverse=True)
    elif sort == "subscribers":
        channels.sort(key=lambda c: c["subscribers"], reverse=True)
    elif sort == "last_post":
        channels.sort(key=lambda c: c["last_post"], reverse=True)

    total = len(channels)
    channels = channels[:limit]

    fields = ["title", "handle", "subscribers", "unread", "type"]
    rows = [
        [ch["title"], ch["handle"], ch["subscribers"], ch["unread"], ch["type"]]
        for ch in channels
    ]

    summary_parts = [f"{total} total"]
    if total > limit:
        summary_parts.append(f"showing {limit}")

    return toon.format_response(
        type_name="channels",
        fields=fields,
        rows=rows,
        summary_parts=summary_parts,
        next_hints=[
            'Channel details: tg_execute op="channel_info" params={"channel": "@handle"}',
            'Channel stats: tg_execute op="channel_stats" params={"channel": "@handle"}',
        ],
    )


# ---------------------------------------------------------------------------
# channel_info
# ---------------------------------------------------------------------------


@operation(
    name="channel_info",
    category="channels",
    description="Detailed info for a single channel: description, admins, creation date, subscriber count, member count",
    destructive=False,
    idempotent=True,
)
async def channel_info(
    client: Any,
    channel: str,
    cache: Cache | None = None,
) -> str:
    """Get detailed info for a single channel."""
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="channel_info" params={"channel": "@llm_under_hood"}',
            recovery="provide a channel identifier",
        )

    channel = channel.strip()

    # Resolve the channel entity
    entity = await _resolve_single_channel(client, channel)

    # Get full channel info (works for Channel entities)
    if isinstance(entity, Channel):
        try:
            full_result = await client(GetFullChannelRequest(entity))
            full_chat = full_result.full_chat
        except ChannelPrivateError:
            raise OperationError(
                what=f"Channel {channel} is private or you were banned",
                expected="accessible channel",
                example='tg_execute op="channel_info" params={"channel": "@public_channel"}',
                recovery="you need to be a member to access this channel",
            )
        except FloodWaitError as e:
            raise TelegramFloodWait(e.seconds) from e
        except Exception as exc:
            logger.exception(
                "ops.channel_info_full_error",
                extra={"channel": channel},
            )
            raise OperationError(
                what=f"Failed to get full info for {channel}: {type(exc).__name__}: {exc}",
                expected="successful channel info fetch",
                example=f'tg_execute op="channel_info" params={{"channel": "{channel}"}}',
                recovery="check channel access and retry",
            ) from exc

        about = getattr(full_chat, "about", None) or ""
        participants_count = getattr(full_chat, "participants_count", None) or 0
        admins_count = getattr(full_chat, "admins_count", None)

        lines = [
            f"channel: {entity.title}",
            f"handle: @{entity.username}" if entity.username else "handle: (none)",
            f"id: {entity.id}",
            f"description: {about}" if about else "description: (none)",
            f"subscribers: {participants_count}",
        ]
        if admins_count is not None:
            lines.append(f"admins: {admins_count}")
        lines.append(f"type: {'channel' if entity.broadcast else 'group'}")
        lines.append(f"verified: {'yes' if entity.verified else 'no'}")
        lines.append(
            f"restricted: {'yes' if entity.restricted else 'no'}"
        )
        if entity.date:
            lines.append(f"created: {toon.format_date(entity.date)}")

        lines.append("")
        lines.append(toon.hint(f'Read messages: tg_feed channel="@{entity.username or entity.id}"'))
        lines.append(
            toon.hint(
                f'Channel stats: tg_execute op="channel_stats" '
                f'params={{"channel": "@{entity.username or entity.id}"}}'
            )
        )

        return "\n".join(lines)

    # For Chat (group) entities — less metadata available
    lines = [
        f"group: {getattr(entity, 'title', '')}",
        f"id: {entity.id}",
        f"type: group",
    ]
    if hasattr(entity, "participants_count") and entity.participants_count:
        lines.append(f"members: {entity.participants_count}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# channel_stats
# ---------------------------------------------------------------------------


@operation(
    name="channel_stats",
    category="channels",
    description="Activity stats for a channel: post frequency, avg views, engagement rate, recent post count",
    destructive=False,
    idempotent=True,
)
async def channel_stats(
    client: Any,
    channel: str,
    days: int = 30,
    cache: Cache | None = None,
) -> str:
    """Get activity statistics for a channel."""
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="channel_stats" params={"channel": "@llm_under_hood"}',
            recovery="provide a channel identifier",
        )

    channel = channel.strip()

    if days < 1 or days > 365:
        raise OperationError(
            what=f"days must be 1-365, got: {days}",
            expected="integer between 1 and 365",
            example='tg_execute op="channel_stats" params={"channel": "@handle", "days": 30}',
            recovery="use a value in the valid range",
        )

    entity = await _resolve_single_channel(client, channel)

    # Fetch recent messages to compute stats
    cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
    messages: list[dict[str, Any]] = []

    try:
        async for msg in client.iter_messages(entity, limit=200):
            if msg is None:
                continue
            if msg.date and msg.date.timestamp() < cutoff:
                break

            views = msg.views or 0
            reactions_count = 0
            if msg.reactions and hasattr(msg.reactions, "results"):
                for r in msg.reactions.results:
                    reactions_count += r.count

            replies = 0
            if msg.replies and hasattr(msg.replies, "replies"):
                replies = msg.replies.replies or 0

            messages.append({
                "views": views,
                "reactions": reactions_count,
                "replies": replies,
                "date": msg.date.isoformat() if msg.date else "",
                "has_text": bool(msg.text),
            })
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except ChannelPrivateError:
        raise OperationError(
            what=f"Channel {channel} is private or you were banned",
            expected="accessible channel",
            example='tg_execute op="channel_stats" params={"channel": "@public_channel"}',
            recovery="you need to be a member to access this channel",
        )
    except Exception as exc:
        logger.exception(
            "ops.channel_stats_fetch_error",
            extra={"channel": channel},
        )
        raise OperationError(
            what=f"Failed to fetch messages for {channel}: {type(exc).__name__}: {exc}",
            expected="successful message fetch",
            example=f'tg_execute op="channel_stats" params={{"channel": "{channel}"}}',
            recovery="check channel access and retry",
        ) from exc

    title = getattr(entity, "title", channel)
    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else title

    if not messages:
        return (
            f"0 messages in {handle_display} in last {days} days.\n"
            f"Channel may be inactive or you may not have access to message history."
        )

    # Compute stats
    total_posts = len(messages)
    total_views = sum(m["views"] for m in messages)
    total_reactions = sum(m["reactions"] for m in messages)
    total_replies = sum(m["replies"] for m in messages)

    avg_views = total_views / total_posts if total_posts else 0
    avg_reactions = total_reactions / total_posts if total_posts else 0
    avg_replies = total_replies / total_posts if total_posts else 0

    weeks = max(days / 7, 1)
    posts_per_week = total_posts / weeks

    # Engagement rate: (reactions + replies) / views
    engagement_rate = 0.0
    if total_views > 0:
        engagement_rate = (total_reactions + total_replies) / total_views * 100

    lines = [
        f"stats: {handle_display} ({days}d window)",
        "",
        f"posts: {total_posts}",
        f"posts/week: {posts_per_week:.1f}",
        f"avg views: {avg_views:.0f}",
        f"avg reactions: {avg_reactions:.1f}",
        f"avg replies: {avg_replies:.1f}",
        f"engagement: {engagement_rate:.2f}%",
        f"total views: {total_views}",
        f"total reactions: {total_reactions}",
        f"total replies: {total_replies}",
    ]

    if messages:
        lines.append(f"first post: {toon.format_date(messages[-1].get('date'))}")
        lines.append(f"latest post: {toon.format_date(messages[0].get('date'))}")

    lines.append("")
    lines.append(toon.hint(f'Read messages: tg_feed channel="{handle_display}"'))
    lines.append(
        toon.hint(
            f'Channel info: tg_execute op="channel_info" '
            f'params={{"channel": "{handle_display}"}}'
        )
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _resolve_single_channel(
    client: Any, identifier: str
) -> Channel | Chat:
    """Resolve a channel identifier to a single entity.

    Raises OperationError if identifier resolves to multiple entities or none.
    """
    # Try @handle or t.me link first
    from tg_mcp.client import TelegramClient as _TgClient

    # Direct resolution via Telethon
    import re

    handle_re = re.compile(r"^@?([a-zA-Z][a-zA-Z0-9_]{3,30}[a-zA-Z0-9])$")
    link_re = re.compile(
        r"^https?://(?:t\.me|telegram\.me)/(?:\+|joinchat/)?([a-zA-Z0-9_]+)$"
    )

    username: str | None = None
    link_match = link_re.match(identifier)
    if link_match:
        username = link_match.group(1)
    else:
        handle_match = handle_re.match(identifier)
        if handle_match:
            username = handle_match.group(1)

    if username is not None:
        try:
            entity = await client.get_entity(username)
        except Exception as exc:
            raise OperationError(
                what=f"Cannot resolve @{username}: {type(exc).__name__}: {exc}",
                expected="valid channel @handle or t.me link",
                example='params={"channel": "@llm_under_hood"}',
                recovery="check the handle spelling or use tg_overview to see channels",
            ) from exc

        if not isinstance(entity, (Channel, Chat)):
            raise OperationError(
                what=f"@{username} is not a channel or group (got {type(entity).__name__})",
                expected="channel or group entity",
                example='params={"channel": "@llm_under_hood"}',
                recovery="provide a channel or group handle, not a user",
            )
        return entity

    # If starts with @ but didn't match handle format
    if identifier.startswith("@"):
        raise OperationError(
            what=f"Invalid handle format: {identifier!r}",
            expected="handles must be 5-32 characters, alphanumeric + underscores, starting with a letter",
            example='params={"channel": "@llm_under_hood"}',
            recovery="check the handle format",
        )

    # Title substring search
    matches: list[Channel | Chat] = []
    identifier_lower = identifier.lower()

    try:
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if not isinstance(entity, (Channel, Chat)):
                continue
            if identifier_lower in dialog.name.lower():
                matches.append(entity)
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e

    if not matches:
        raise OperationError(
            what=f"No channel matches {identifier!r}",
            expected="channel title substring matching a subscribed channel",
            example='params={"channel": "LLM Under"}',
            recovery="check spelling or use tg_overview to see all channels",
        )

    if len(matches) > 1:
        names = [getattr(m, "title", "?") for m in matches[:5]]
        raise OperationError(
            what=f"Multiple channels match {identifier!r}: {', '.join(names)}",
            expected="unambiguous channel identifier",
            example='params={"channel": "@exact_handle"}',
            recovery="use exact @handle to disambiguate",
        )

    return matches[0]
