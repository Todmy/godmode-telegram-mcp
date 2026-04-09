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
from telethon.tl.types import ReactionEmoji

from tg_mcp import toon
from tg_mcp.cache import Cache
from tg_mcp.catalog import OperationError, operation
from tg_mcp.client import TelegramFloodWait
from tg_mcp.config import logger
from tg_mcp.ops.channels import _resolve_single_channel


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
    description="Mark all messages in a channel as read. Idempotent — safe to call on already-read channels",
    destructive=False,
    idempotent=True,
)
async def mark_read(
    client: Any,
    channel: str,
    cache: Cache | None = None,
) -> str:
    """Mark all messages in a channel as read."""
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

    # --- Mark read ---
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

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)

    if unread_before == 0:
        return f"{handle_display}: already read (0 unread)."

    return f"Marked {handle_display} as read ({unread_before} messages were unread)."
