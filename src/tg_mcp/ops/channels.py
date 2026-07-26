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
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.functions.channels import (
    GetFullChannelRequest,
    JoinChannelRequest,
    LeaveChannelRequest,
)
from telethon.tl.types import Channel, Chat, InputNotifyPeer, InputPeerNotifySettings, User

from tg_mcp import toon
from tg_mcp.cache import Cache, CacheCategory, invalidate_keys, make_cache_key
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
    valid_types = {"channels", "groups", "private", "all"}
    if type not in valid_types:
        raise OperationError(
            what=f"Invalid type filter: {type!r}",
            expected=f"one of: {', '.join(sorted(valid_types))}",
            example='tg_execute op="list_channels" params={"type": "private"}',
            recovery="use 'channels', 'groups', 'private', or 'all'",
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
            is_private = isinstance(entity, User)
            is_channel_or_group = isinstance(entity, (Channel, Chat))

            if not is_private and not is_channel_or_group:
                continue

            if is_private:
                entity_type = "private"
            elif isinstance(entity, Channel) and entity.broadcast:
                entity_type = "channel"
            else:
                entity_type = "group"

            if type == "channels" and entity_type != "channel":
                continue
            if type == "groups" and entity_type != "group":
                continue
            if type == "private" and entity_type != "private":
                continue

            handle = getattr(entity, "username", None)
            subscribers = getattr(entity, "participants_count", None) or 0

            if is_private:
                first = getattr(entity, "first_name", "") or ""
                last = getattr(entity, "last_name", "") or ""
                title = dialog.name or f"{first} {last}".strip()
            else:
                title = dialog.name or getattr(entity, "title", "")

            channels.append({
                "title": title,
                "handle": f"@{handle}" if handle else "",
                "subscribers": subscribers,
                "unread": dialog.unread_count or 0,
                "last_post": dialog.date.isoformat() if dialog.date else "",
                "type": entity_type,
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

    # Private chat (User) — no title, no participants; report identity instead
    if isinstance(entity, User):
        first = getattr(entity, "first_name", "") or ""
        last = getattr(entity, "last_name", "") or ""
        name = f"{first} {last}".strip()
        username = getattr(entity, "username", None)

        lines = [
            f"private chat: {name or (f'@{username}' if username else entity.id)}",
            f"handle: @{username}" if username else "handle: (none)",
            f"id: {entity.id}",
            "type: private",
        ]
        if getattr(entity, "phone", None):
            lines.append(f"phone: +{entity.phone}")
        lines.append(f"bot: {'yes' if getattr(entity, 'bot', False) else 'no'}")
        lines.append(f"contact: {'yes' if getattr(entity, 'contact', False) else 'no'}")
        lines.append(f"verified: {'yes' if getattr(entity, 'verified', False) else 'no'}")

        target = f"@{username}" if username else str(entity.id)
        lines.append("")
        lines.append(toon.hint(f'Read messages: tg_feed channel="{target}"'))
        lines.append(
            toon.hint(
                f'Send a message: tg_execute op="send_message" '
                f'params={{"chat": "{target}", "text": "..."}} confirm=true'
            )
        )

        return "\n".join(lines)

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
# subscribe (T031)
# ---------------------------------------------------------------------------


@operation(
    name="subscribe",
    category="channels",
    description="Join/subscribe to a channel by @handle or t.me link",
    destructive=False,
    idempotent=True,
)
async def subscribe(
    client: Any,
    channel: str,
    cache: Cache | None = None,
) -> str:
    """Join a Telegram channel."""
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle or t.me link",
            example='tg_execute op="subscribe" params={"channel": "@llm_under_hood"}',
            recovery="provide a channel identifier",
        )

    channel = channel.strip()

    # Resolve entity (may already be subscribed — that's fine, idempotent)
    try:
        entity = await _resolve_single_channel(client, channel)
    except OperationError:
        # If resolution fails, try direct get_entity for unsubscribed channels
        import re
        handle_re = re.compile(r"^@?([a-zA-Z][a-zA-Z0-9_]{3,30}[a-zA-Z0-9])$")
        link_re = re.compile(
            r"^https?://(?:t\.me|telegram\.me)/(?:\+|joinchat/)?([a-zA-Z0-9_]+)$"
        )
        username = None
        link_match = link_re.match(channel)
        if link_match:
            username = link_match.group(1)
        else:
            handle_match = handle_re.match(channel)
            if handle_match:
                username = handle_match.group(1)

        if username is None:
            raise OperationError(
                what=f"Cannot resolve {channel!r} — need @handle or t.me link to subscribe",
                expected="@handle or t.me link",
                example='tg_execute op="subscribe" params={"channel": "@llm_under_hood"}',
                recovery="provide a valid @handle or t.me link",
            )

        try:
            entity = await client.get_entity(username)
        except Exception as exc:
            raise OperationError(
                what=f"Channel @{username} not found: {type(exc).__name__}: {exc}",
                expected="existing public channel",
                example='tg_execute op="subscribe" params={"channel": "@llm_under_hood"}',
                recovery="check the handle spelling",
            ) from exc

    try:
        await client(JoinChannelRequest(entity))
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except ChannelPrivateError:
        raise OperationError(
            what=f"Channel {channel} is private — cannot join without invite",
            expected="public channel or valid invite link",
            example='tg_execute op="subscribe" params={"channel": "@public_channel"}',
            recovery="use a t.me/+invite link for private channels",
        )
    except Exception as exc:
        logger.exception("ops.subscribe_error", extra={"channel": channel})
        raise OperationError(
            what=f"Failed to join {channel}: {type(exc).__name__}: {exc}",
            expected="successful channel join",
            example=f'tg_execute op="subscribe" params={{"channel": "{channel}"}}',
            recovery="check that the channel exists and is accessible",
        ) from exc

    await invalidate_keys(cache, make_cache_key("channels"))

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)
    title = getattr(entity, "title", "")

    lines = [f"Subscribed to {title} ({handle_display})."]
    lines.append("")
    lines.append(toon.hint(f'Read messages: tg_feed channel="{handle_display}"'))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# unsubscribe (T031)
# ---------------------------------------------------------------------------


@operation(
    name="unsubscribe",
    category="channels",
    description="Leave/unsubscribe from a channel. This is destructive — you may lose access to private channels",
    destructive=True,
    idempotent=True,
)
async def unsubscribe(
    client: Any,
    channel: str,
    cache: Cache | None = None,
) -> str:
    """Leave a Telegram channel."""
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="unsubscribe" params={"channel": "@some_channel"} confirm=true',
            recovery="provide a channel identifier",
        )

    channel = channel.strip()
    # Destructive: never let a fuzzy title sweep into a private chat.
    entity = await _resolve_single_channel(client, channel, allow_users=False)

    try:
        await client(LeaveChannelRequest(entity))
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.unsubscribe_error", extra={"channel": channel})
        raise OperationError(
            what=f"Failed to leave {channel}: {type(exc).__name__}: {exc}",
            expected="successful channel leave",
            example=f'tg_execute op="unsubscribe" params={{"channel": "{channel}"}} confirm=true',
            recovery="check channel access and retry",
        ) from exc

    await invalidate_keys(cache, make_cache_key("channels"))

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)

    return f"Unsubscribed from {handle_display}."


# ---------------------------------------------------------------------------
# mute_channel (T031)
# ---------------------------------------------------------------------------


@operation(
    name="mute_channel",
    category="channels",
    description="Mute or unmute a channel. Muted channels won't send push notifications",
    destructive=False,
    idempotent=True,
)
async def mute_channel(
    client: Any,
    channel: str,
    mute: bool = True,
    cache: Cache | None = None,
) -> str:
    """Mute or unmute a channel."""
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="mute_channel" params={"channel": "@llm_under_hood", "mute": true}',
            recovery="provide a channel identifier",
        )

    channel = channel.strip()
    entity = await _resolve_single_channel(client, channel)

    # mute_until = max int32 means "forever". 0 means unmuted.
    mute_until = 2**31 - 1 if mute else 0

    try:
        await client(UpdateNotifySettingsRequest(
            peer=InputNotifyPeer(entity),
            settings=InputPeerNotifySettings(mute_until=mute_until),
        ))
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.mute_channel_error", extra={"channel": channel, "mute": mute})
        raise OperationError(
            what=f"Failed to {'mute' if mute else 'unmute'} {channel}: {type(exc).__name__}: {exc}",
            expected="successful notification settings update",
            example=f'tg_execute op="mute_channel" params={{"channel": "{channel}", "mute": {str(mute).lower()}}}',
            recovery="check channel access and retry",
        ) from exc

    await invalidate_keys(cache, make_cache_key("channels"))

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)
    action = "Muted" if mute else "Unmuted"

    return f"{action} {handle_display}."


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _entity_display(entity: Any) -> str:
    """Human-readable name for a Channel/Chat/User entity."""
    handle = getattr(entity, "username", None)

    if isinstance(entity, User):
        first = getattr(entity, "first_name", "") or ""
        last = getattr(entity, "last_name", "") or ""
        name = f"{first} {last}".strip()
        if not name:
            return f"@{handle}" if handle else f"id:{entity.id}"
        return f"{name} (@{handle})" if handle else name

    if handle:
        return f"@{handle}"
    return getattr(entity, "title", "") or f"id:{getattr(entity, 'id', '?')}"


async def _resolve_single_channel(
    client: Any,
    identifier: str,
    *,
    allow_users: bool = True,
    allow_user_fuzzy: bool = True,
) -> Channel | Chat | User:
    """Resolve a channel/group/user identifier to a single entity.

    Args:
        allow_users: when False, private chats (User entities) are rejected
            outright — used by operations that only make sense on channels.
        allow_user_fuzzy: when False, title-substring search never matches a
            private chat. Destructive operations set this so that a vague
            title like "Marketing" can never silently resolve to a contact;
            DMs must then be named by exact @handle or numeric id.

    Raises OperationError if identifier resolves to multiple entities or none.
    """
    import re

    handle_re = re.compile(r"^@?([a-zA-Z][a-zA-Z0-9_]{3,30}[a-zA-Z0-9])$")
    link_re = re.compile(
        r"^https?://(?:t\.me|telegram\.me)/(?:\+|joinchat/)?([a-zA-Z0-9_]+)$"
    )
    id_re = re.compile(r"^-?\d+$")

    # Exact numeric id — the unambiguous way to name a private chat
    if id_re.match(identifier):
        try:
            entity = await client.get_entity(int(identifier))
        except Exception as exc:
            raise OperationError(
                what=f"Cannot resolve id {identifier}: {type(exc).__name__}: {exc}",
                expected="an id of a dialog you have seen in this session",
                example='params={"channel": "@llm_under_hood"}',
                recovery="use tg_overview to find the dialog, then pass its @handle",
            ) from exc

        if isinstance(entity, User) and not allow_users:
            raise OperationError(
                what=f"id {identifier} is a private chat, not a channel or group",
                expected="channel or group entity",
                example='params={"channel": "@llm_under_hood"}',
                recovery="this operation does not apply to private chats",
            )
        if not isinstance(entity, (Channel, Chat, User)):
            raise OperationError(
                what=f"id {identifier} resolved to unexpected type: {type(entity).__name__}",
                expected="channel, group, or user entity",
                example='params={"channel": "@llm_under_hood"}',
                recovery="provide a valid handle or id",
            )
        return entity

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
                expected="valid @handle or t.me link",
                example='params={"channel": "@llm_under_hood"}',
                recovery="check the handle spelling or use tg_overview to see channels",
            ) from exc

        if isinstance(entity, User) and not allow_users:
            raise OperationError(
                what=f"@{username} is a user account, not a channel or group",
                expected="channel or group entity",
                example='params={"channel": "@llm_under_hood"}',
                recovery="this operation does not apply to private chats",
            )

        if not isinstance(entity, (Channel, Chat, User)):
            raise OperationError(
                what=f"@{username} resolved to unexpected type: {type(entity).__name__}",
                expected="channel, group, or user entity",
                example='params={"channel": "@llm_under_hood"}',
                recovery="provide a valid handle",
            )
        return entity

    if identifier.startswith("@"):
        raise OperationError(
            what=f"Invalid handle format: {identifier!r}",
            expected="handles must be 5-32 characters, alphanumeric + underscores, starting with a letter",
            example='params={"channel": "@llm_under_hood"}',
            recovery="check the handle format",
        )

    # Title/name substring search across dialogs.
    # Private chats participate only when fuzzy user matching is allowed.
    allowed_types: tuple[type, ...] = (
        (Channel, Chat, User) if allow_users and allow_user_fuzzy else (Channel, Chat)
    )
    matches: list[Channel | Chat | User] = []
    identifier_lower = identifier.lower()

    try:
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if not isinstance(entity, allowed_types):
                continue
            if identifier_lower in dialog.name.lower():
                matches.append(entity)
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e

    if not matches:
        raise OperationError(
            what=f"No dialog matches {identifier!r}",
            expected="title/name substring matching a dialog"
            if User in allowed_types
            else "title substring matching a channel or group "
            "(private chats must be named by exact @handle or id here)",
            example='params={"channel": "LLM Under"}',
            recovery="check spelling or use tg_overview to see all dialogs",
        )

    if len(matches) > 1:
        names = [_entity_display(m) for m in matches[:5]]
        raise OperationError(
            what=f"Multiple dialogs match {identifier!r}: {', '.join(names)}",
            expected="unambiguous identifier",
            example='params={"channel": "@exact_handle"}',
            recovery="use exact @handle or more specific title substring to disambiguate",
        )

    return matches[0]
