"""Interaction operations — react, comment, forward, mark read.

Write operations that modify state in Telegram.
Registered into the catalog via @operation() decorator.
"""

from __future__ import annotations

from typing import Any

from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    MessageIdInvalidError,
    MsgIdInvalidError,
    ReactionInvalidError,
)
from telethon.tl.functions.messages import (
    GetForumTopicsRequest,
    ReadDiscussionRequest,
)
from telethon.tl.types import ReactionEmoji

from tg_mcp import toon
from tg_mcp.cache import Cache
from tg_mcp.catalog import OperationError, operation
from tg_mcp.client import TelegramFloodWait
from tg_mcp.config import logger
from tg_mcp.ops.channels import _entity_display, _resolve_single_channel


# ---------------------------------------------------------------------------
# send_message
# ---------------------------------------------------------------------------

_SAVED_MESSAGES_ALIASES = {"me", "self", "saved", "saved messages"}


async def _resolve_message_target(client: Any, chat: str) -> tuple[Any, str]:
    """Resolve a send target to (entity, display name).

    Private chats are never matched by fuzzy title substring here — sending a
    message is irreversible, so a DM must be named by exact @handle or id.
    """
    if chat.lower() in _SAVED_MESSAGES_ALIASES:
        entity = await client.get_entity("me")
        return entity, "Saved Messages"

    entity = await _resolve_single_channel(client, chat, allow_user_fuzzy=False)
    return entity, _entity_display(entity)


@operation(
    name="send_message",
    category="interact",
    description=(
        "Send a new message to a channel, group, or private chat (DM). "
        "Destructive: the message reaches a real recipient and cannot be "
        "unsent from here, so confirm=true is required"
    ),
    destructive=True,
    idempotent=False,
)
async def send_message(
    client: Any,
    chat: str,
    text: str,
    reply_to: int = 0,
    cache: Cache | None = None,
) -> str:
    """Send a message to a chat, optionally as a reply."""
    # --- Input validation ---
    if not chat or not chat.strip():
        raise OperationError(
            what="chat parameter is required",
            expected="@handle, numeric id, chat title, or 'me' for Saved Messages",
            example='tg_execute op="send_message" params={"chat": "me", "text": "note to self"} confirm=true',
            recovery="provide a chat identifier",
        )

    if not text or not text.strip():
        raise OperationError(
            what="text parameter is required and cannot be empty",
            expected="non-empty message text",
            example='tg_execute op="send_message" params={"chat": "me", "text": "hello"} confirm=true',
            recovery="provide the message content",
        )

    if reply_to < 0:
        raise OperationError(
            what=f"reply_to must be a positive message id or 0 for none, got: {reply_to}",
            expected="valid Telegram message ID",
            example='tg_execute op="send_message" params={"chat": "me", "text": "hi", "reply_to": 123} confirm=true',
            recovery="use a message ID from search or feed results, or omit reply_to",
        )

    chat = chat.strip()
    text = text.strip()

    entity, target_display = await _resolve_message_target(client, chat)

    # --- Send ---
    try:
        result = await client.send_message(
            entity,
            text,
            reply_to=reply_to or None,
        )
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except (MsgIdInvalidError, MessageIdInvalidError):
        raise OperationError(
            what=f"Reply target message {reply_to} not found in {target_display}",
            expected="existing message ID in the same chat",
            example=f'tg_execute op="search_messages" params={{"channel": "{chat}", "query": "keyword"}}',
            recovery="the message may have been deleted — omit reply_to or pick another id",
        )
    except ChannelPrivateError:
        raise OperationError(
            what=f"Chat {chat} is private or you were banned",
            expected="accessible chat",
            example='tg_execute op="send_message" params={"chat": "me", "text": "hello"} confirm=true',
            recovery="you need to be a member to post in this chat",
        )
    except Exception as exc:
        logger.exception(
            "ops.send_message_error",
            extra={"chat": chat, "reply_to": reply_to},
        )
        raise OperationError(
            what=f"Failed to send message: {type(exc).__name__}: {exc}",
            expected="successful message delivery",
            example=f'tg_execute op="send_message" params={{"chat": "{chat}", "text": "hello"}} confirm=true',
            recovery="check that you can post in this chat and retry",
        ) from exc

    message_id = getattr(result, "id", 0)
    timestamp = toon.format_date(getattr(result, "date", None))

    lines = [
        f"Sent message {message_id} to {target_display}.",
        "",
        f"message_id: {message_id}",
        f"chat: {target_display}",
        f"timestamp: {timestamp}",
    ]
    if reply_to:
        lines.append(f"reply_to: {reply_to}")
    lines.append("text:")
    lines.append(text)

    return "\n".join(lines)


async def _send_message_preview(client: Any, params: dict[str, Any]) -> str:
    """Confirmation preview: resolved recipient plus the exact text to send."""
    chat = str(params.get("chat") or "").strip()
    text = str(params.get("text") or "")
    reply_to = params.get("reply_to")

    if not chat or not text.strip():
        return ""

    _entity, target_display = await _resolve_message_target(client, chat)

    lines = [f"Will send to: {target_display}"]
    if reply_to:
        lines.append(f"As a reply to message: {reply_to}")
    lines.append(f"Message text ({len(text)} chars):")
    lines.append("---")
    lines.append(text)
    lines.append("---")
    lines.append("This reaches a real recipient and cannot be undone from here.")
    return "\n".join(lines)


send_message.confirm_preview = _send_message_preview


# ---------------------------------------------------------------------------
# react_to_message (T025)
# ---------------------------------------------------------------------------


@operation(
    name="react_to_message",
    category="interact",
    description="Add an emoji reaction to a message. Common emoji: thumbs up, heart, fire, clap, etc.",
    destructive=False,
    idempotent=True,
)
async def react_to_message(
    client: Any,
    channel: str,
    message_id: int,
    emoji: str = "\U0001f44d",
    cache: Cache | None = None,
) -> str:
    """Add a reaction to a message."""
    # --- Input validation ---
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="react_to_message" params={"channel": "@llm_under_hood", "message_id": 123, "emoji": "\U0001f44d"}',
            recovery="provide a channel identifier",
        )

    if message_id < 1:
        raise OperationError(
            what=f"message_id must be a positive integer, got: {message_id}",
            expected="valid Telegram message ID",
            example='tg_execute op="react_to_message" params={"channel": "@handle", "message_id": 123}',
            recovery="use a message ID from search or feed results",
        )

    if not emoji or not emoji.strip():
        raise OperationError(
            what="emoji parameter cannot be empty",
            expected="single emoji character",
            example='tg_execute op="react_to_message" params={"channel": "@handle", "message_id": 123, "emoji": "\U0001f44d"}',
            recovery="provide an emoji like \U0001f44d \u2764\ufe0f \U0001f525 \U0001f44f",
        )

    emoji = emoji.strip()
    channel = channel.strip()
    entity = await _resolve_single_channel(client, channel)

    # --- Send reaction ---
    try:
        await client.send_reaction(entity, message_id, [ReactionEmoji(emoticon=emoji)])
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except ReactionInvalidError:
        raise OperationError(
            what=f"Telegram rejected the reaction emoji: {emoji!r}",
            expected="emoji that this channel allows as a reaction",
            example='tg_execute op="react_to_message" params={"channel": "@handle", "message_id": 123, "emoji": "\U0001f44d"}',
            recovery="use a standard emoji: \U0001f44d \u2764\ufe0f \U0001f525 \U0001f44f \U0001f602 \U0001f622 \U0001f92f \U0001f914 \U0001f4af",
        )
    except (MsgIdInvalidError, MessageIdInvalidError):
        raise OperationError(
            what=f"Message {message_id} not found in {channel}",
            expected="existing message ID",
            example=f'tg_execute op="search_messages" params={{"channel": "{channel}", "query": "keyword"}}',
            recovery="the message may have been deleted — search for it by content",
        )
    except ChannelPrivateError:
        raise OperationError(
            what=f"Channel {channel} is private or you were banned",
            expected="accessible channel",
            example='tg_execute op="react_to_message" params={"channel": "@public_channel", "message_id": 1, "emoji": "\U0001f44d"}',
            recovery="you need to be a member to react in this channel",
        )
    except Exception as exc:
        logger.exception(
            "ops.react_to_message_error",
            extra={"channel": channel, "message_id": message_id, "emoji": emoji},
        )
        raise OperationError(
            what=f"Failed to react: {type(exc).__name__}: {exc}",
            expected="successful reaction",
            example=f'tg_execute op="react_to_message" params={{"channel": "{channel}", "message_id": {message_id}, "emoji": "\U0001f44d"}}',
            recovery="check that the channel allows reactions and retry",
        ) from exc

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)

    return f"Reacted {emoji} to message {message_id} in {handle_display}."


# ---------------------------------------------------------------------------
# send_comment (T026)
# ---------------------------------------------------------------------------


@operation(
    name="send_comment",
    category="interact",
    description="Send a comment on a channel message (requires linked discussion group). The comment appears in the channel's discussion thread",
    destructive=False,
    idempotent=False,
)
async def send_comment(
    client: Any,
    channel: str,
    message_id: int,
    text: str,
    cache: Cache | None = None,
) -> str:
    """Comment on a channel message via discussion group."""
    # --- Input validation ---
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="send_comment" params={"channel": "@llm_under_hood", "message_id": 123, "text": "Great post!"}',
            recovery="provide a channel identifier",
        )

    if message_id < 1:
        raise OperationError(
            what=f"message_id must be a positive integer, got: {message_id}",
            expected="valid Telegram message ID",
            example='tg_execute op="send_comment" params={"channel": "@handle", "message_id": 123, "text": "comment"}',
            recovery="use a message ID from search or feed results",
        )

    if not text or not text.strip():
        raise OperationError(
            what="text parameter is required and cannot be empty",
            expected="non-empty comment text",
            example='tg_execute op="send_comment" params={"channel": "@handle", "message_id": 123, "text": "Great insight!"}',
            recovery="provide the comment content",
        )

    text = text.strip()
    channel = channel.strip()
    entity = await _resolve_single_channel(client, channel)

    # --- Send comment ---
    try:
        result = await client.send_message(entity, text, comment_to=message_id)
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except (MsgIdInvalidError, MessageIdInvalidError):
        raise OperationError(
            what=f"Message {message_id} not found in {channel}",
            expected="existing message ID",
            example=f'tg_execute op="search_messages" params={{"channel": "{channel}", "query": "keyword"}}',
            recovery="the message may have been deleted — search for it by content",
        )
    except ChannelPrivateError:
        raise OperationError(
            what=f"Channel {channel} is private or you were banned",
            expected="accessible channel",
            example='tg_execute op="send_comment" params={"channel": "@public_channel", "message_id": 1, "text": "comment"}',
            recovery="you need to be a member to comment in this channel",
        )
    except Exception as exc:
        # Telegram returns various errors when discussion group is missing
        exc_msg = str(exc).lower()
        if "discussion" in exc_msg or "comments" in exc_msg or "peer_id_invalid" in exc_msg:
            raise OperationError(
                what=f"Channel {channel} does not have a linked discussion group",
                expected="channel with discussion/comments enabled",
                example='tg_execute op="send_comment" params={"channel": "@channel_with_comments", "message_id": 1, "text": "comment"}',
                recovery="comments are only possible on channels that have a linked discussion group — check channel settings",
            ) from exc

        logger.exception(
            "ops.send_comment_error",
            extra={"channel": channel, "message_id": message_id},
        )
        raise OperationError(
            what=f"Failed to send comment: {type(exc).__name__}: {exc}",
            expected="successful comment delivery",
            example=f'tg_execute op="send_comment" params={{"channel": "{channel}", "message_id": {message_id}, "text": "comment"}}',
            recovery="check that the channel allows comments and retry",
        ) from exc

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)
    comment_id = getattr(result, "id", None)

    lines = [
        f"Comment sent to message {message_id} in {handle_display}.",
    ]
    if comment_id:
        lines.append(f"comment_id: {comment_id}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# forward_message (T027)
# ---------------------------------------------------------------------------


@operation(
    name="forward_message",
    category="interact",
    description="Forward a message to Saved Messages (default) or a specified chat",
    destructive=False,
    idempotent=True,
)
async def forward_message(
    client: Any,
    channel: str,
    message_id: int,
    to: str = "me",
    cache: Cache | None = None,
) -> str:
    """Forward a message to Saved Messages or a specified chat."""
    # --- Input validation ---
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="forward_message" params={"channel": "@llm_under_hood", "message_id": 123}',
            recovery="provide a channel identifier",
        )

    if message_id < 1:
        raise OperationError(
            what=f"message_id must be a positive integer, got: {message_id}",
            expected="valid Telegram message ID",
            example='tg_execute op="forward_message" params={"channel": "@handle", "message_id": 123}',
            recovery="use a message ID from search or feed results",
        )

    if not to or not to.strip():
        raise OperationError(
            what="to parameter cannot be empty — use 'me' for Saved Messages",
            expected="'me' (Saved Messages) or a @handle / chat title",
            example='tg_execute op="forward_message" params={"channel": "@handle", "message_id": 123, "to": "me"}',
            recovery="use 'me' to forward to Saved Messages",
        )

    channel = channel.strip()
    to = to.strip()

    entity = await _resolve_single_channel(client, channel)

    # Resolve target: "me" = Saved Messages, otherwise resolve as entity
    target: Any
    target_display: str
    if to.lower() == "me":
        target = "me"
        target_display = "Saved Messages"
    else:
        try:
            target = await _resolve_single_channel(client, to)
            target_handle = getattr(target, "username", None)
            target_display = f"@{target_handle}" if target_handle else getattr(target, "title", to)
        except OperationError:
            # Re-raise with forward-specific context
            raise OperationError(
                what=f"Target chat {to!r} not found",
                expected="'me' (Saved Messages) or a valid @handle / chat title",
                example='tg_execute op="forward_message" params={"channel": "@handle", "message_id": 123, "to": "me"}',
                recovery="use 'me' for Saved Messages, or provide a valid chat handle",
            )

    # --- Forward ---
    try:
        await client.forward_messages(target, message_id, entity)
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except (MsgIdInvalidError, MessageIdInvalidError):
        raise OperationError(
            what=f"Message {message_id} not found in {channel}",
            expected="existing message ID",
            example=f'tg_execute op="search_messages" params={{"channel": "{channel}", "query": "keyword"}}',
            recovery="the message may have been deleted — search for it by content",
        )
    except ChannelPrivateError:
        raise OperationError(
            what=f"Channel {channel} is private or you were banned",
            expected="accessible channel",
            example='tg_execute op="forward_message" params={"channel": "@public_channel", "message_id": 1}',
            recovery="you need to be a member to forward from this channel",
        )
    except Exception as exc:
        logger.exception(
            "ops.forward_message_error",
            extra={"channel": channel, "message_id": message_id, "to": to},
        )
        raise OperationError(
            what=f"Failed to forward: {type(exc).__name__}: {exc}",
            expected="successful message forward",
            example=f'tg_execute op="forward_message" params={{"channel": "{channel}", "message_id": {message_id}, "to": "me"}}',
            recovery="check source and target access, then retry",
        ) from exc

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)

    return f"Forwarded message {message_id} from {handle_display} to {target_display}."


# ---------------------------------------------------------------------------
# mark_read (T028)
# ---------------------------------------------------------------------------


@operation(
    name="mark_read",
    category="interact",
    description="Mark all messages in a channel as read, including forum topics in supergroups. Idempotent — safe to call on already-read channels",
    destructive=False,
    idempotent=True,
)
async def mark_read(
    client: Any,
    channel: str,
    cache: Cache | None = None,
) -> str:
    """Mark all messages in a channel as read.

    For forum supergroups, also marks every topic with unread messages as read
    via ReadDiscussionRequest. Without this, the main channel read acknowledgement
    leaves per-topic unread badges intact.
    """
    # --- Input validation ---
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="mark_read" params={"channel": "@llm_under_hood"}',
            recovery="provide a channel identifier",
        )

    channel = channel.strip()
    entity = await _resolve_single_channel(client, channel)

    # Check current unread count before marking
    unread_before: int = 0
    try:
        async for dialog in client.iter_dialogs():
            if dialog.entity and dialog.entity.id == entity.id:
                unread_before = dialog.unread_count or 0
                break
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception:
        # Non-critical: proceed even if we can't get unread count
        pass

    # --- Mark channel-level read ---
    try:
        await client.send_read_acknowledge(entity)
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except ChannelPrivateError:
        raise OperationError(
            what=f"Channel {channel} is private or you were banned",
            expected="accessible channel",
            example='tg_execute op="mark_read" params={"channel": "@public_channel"}',
            recovery="you need to be a member to mark messages as read",
        )
    except Exception as exc:
        logger.exception("ops.mark_read_error", extra={"channel": channel})
        raise OperationError(
            what=f"Failed to mark as read: {type(exc).__name__}: {exc}",
            expected="successful read acknowledgement",
            example=f'tg_execute op="mark_read" params={{"channel": "{channel}"}}',
            recovery="check channel access and retry",
        ) from exc

    # --- Forum supergroups: mark each unread topic as read ---
    topics_cleared = 0
    topic_unread_total = 0
    is_forum = bool(getattr(entity, "forum", False))

    if is_forum:
        try:
            forum_result = await client(GetForumTopicsRequest(
                peer=entity,
                offset_date=None,
                offset_id=0,
                offset_topic=0,
                limit=100,
            ))
            for topic in getattr(forum_result, "topics", []) or []:
                unread = getattr(topic, "unread_count", 0) or 0
                if unread <= 0:
                    continue
                try:
                    await client(ReadDiscussionRequest(
                        peer=entity,
                        msg_id=topic.id,
                        read_max_id=topic.top_message,
                    ))
                    topics_cleared += 1
                    topic_unread_total += unread
                except FloodWaitError as e:
                    raise TelegramFloodWait(e.seconds) from e
                except Exception:
                    logger.exception(
                        "ops.mark_read_topic_error",
                        extra={
                            "channel": channel,
                            "topic_id": getattr(topic, "id", None),
                            "topic_title": getattr(topic, "title", None),
                        },
                    )
        except FloodWaitError as e:
            raise TelegramFloodWait(e.seconds) from e
        except Exception:
            # Topic enumeration failed — main channel is already marked read.
            # Log and proceed; partial success is better than raising.
            logger.exception("ops.mark_read_topics_enum_error", extra={"channel": channel})

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)

    # --- Format response ---
    if unread_before == 0 and topics_cleared == 0:
        return f"{handle_display}: already read (0 unread)."

    parts = []
    if unread_before > 0:
        parts.append(f"{unread_before} channel messages")
    if topics_cleared > 0:
        parts.append(f"{topic_unread_total} messages across {topics_cleared} topics")

    if not parts:
        return f"Marked {handle_display} as read."

    return f"Marked {handle_display} as read ({' + '.join(parts)})."


# ---------------------------------------------------------------------------
# list_topics (T029)
# ---------------------------------------------------------------------------


@operation(
    name="list_topics",
    category="channels",
    description="List forum topics in a supergroup. Returns topic id, title, top_message, and unread_count. Empty list if channel is not a forum.",
    destructive=False,
    idempotent=True,
)
async def list_topics(
    client: Any,
    channel: str,
    limit: int = 100,
    cache: Cache | None = None,
) -> str:
    """List forum topics in a supergroup."""
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="list_topics" params={"channel": "@UkrainianStartups"}',
            recovery="provide a channel identifier",
        )

    if limit < 1 or limit > 100:
        raise OperationError(
            what=f"limit must be between 1 and 100, got: {limit}",
            expected="integer 1-100",
            example='tg_execute op="list_topics" params={"channel": "@handle", "limit": 50}',
            recovery="use a limit between 1 and 100",
        )

    channel = channel.strip()
    entity = await _resolve_single_channel(client, channel)

    is_forum = bool(getattr(entity, "forum", False))
    if not is_forum:
        handle = getattr(entity, "username", None)
        display = f"@{handle}" if handle else getattr(entity, "title", channel)
        return f"{display} is not a forum supergroup — no topics."

    try:
        result = await client(GetForumTopicsRequest(
            peer=entity,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=limit,
        ))
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.list_topics_error", extra={"channel": channel})
        raise OperationError(
            what=f"Failed to list topics: {type(exc).__name__}: {exc}",
            expected="successful topic listing",
            example=f'tg_execute op="list_topics" params={{"channel": "{channel}"}}',
            recovery="check channel access and retry",
        ) from exc

    topics = getattr(result, "topics", []) or []
    if not topics:
        return "No topics found."

    rows = []
    for t in topics:
        rows.append({
            "id": t.id,
            "title": getattr(t, "title", "") or "",
            "top_message": getattr(t, "top_message", 0) or 0,
            "unread": getattr(t, "unread_count", 0) or 0,
        })

    total_unread = sum(r["unread"] for r in rows)
    header = f"topics[{len(rows)}]{{id,title,top_message,unread}}:"
    lines = [header]
    for r in rows:
        lines.append(f"{r['id']},{r['title']},{r['top_message']},{r['unread']}")
    lines.append(f"\nsummary: {len(rows)} topics | {total_unread} unread")
    return "\n".join(lines)
