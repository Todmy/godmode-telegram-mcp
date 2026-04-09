"""Folder operations — list, contents, move, create.

Manage Telegram dialog filters (folders).
Registered into the catalog via @operation() decorator.
"""

from __future__ import annotations

from typing import Any

from telethon.errors import FloodWaitError
from telethon.tl.functions.messages import (
    GetDialogFiltersRequest,
    UpdateDialogFilterRequest,
)
from telethon.tl.types import Channel, Chat

from tg_mcp import toon
from tg_mcp.cache import Cache
from tg_mcp.catalog import OperationError, operation
from tg_mcp.client import TelegramFloodWait
from tg_mcp.config import logger
from tg_mcp.ops.channels import _resolve_single_channel


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_folder_info(f: Any) -> dict[str, Any] | None:
    """Extract folder metadata from a Telethon DialogFilter object.

    Returns None for non-standard filter types (e.g. DialogFilterDefault).
    """
    # DialogFilterDefault and similar don't have an 'id' attribute we need
    if not hasattr(f, "id") or not hasattr(f, "title"):
        return None

    title = f.title
    # Telethon may wrap title in a TextWithEntities-like object
    if hasattr(title, "text"):
        title = title.text

    include_peers = getattr(f, "include_peers", []) or []
    return {
        "id": f.id,
        "title": str(title),
        "channel_count": len(include_peers),
        "include_peers": include_peers,
    }


def _get_peer_id(peer: Any) -> int | None:
    """Extract numeric ID from various Telegram peer types."""
    if hasattr(peer, "channel_id"):
        return peer.channel_id
    if hasattr(peer, "chat_id"):
        return peer.chat_id
    if hasattr(peer, "user_id"):
        return peer.user_id
    return None


# ---------------------------------------------------------------------------
# list_folders (T029)
# ---------------------------------------------------------------------------


@operation(
    name="list_folders",
    category="folders",
    description="List all Telegram folders with channel counts",
    destructive=False,
    idempotent=True,
)
async def list_folders(
    client: Any,
    cache: Cache | None = None,
) -> str:
    """List all Telegram folders with channel counts."""
    try:
        result = await client(GetDialogFiltersRequest())
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.list_folders_error")
        raise OperationError(
            what=f"Failed to fetch folders: {type(exc).__name__}: {exc}",
            expected="successful folder list fetch",
            example='tg_execute op="list_folders"',
            recovery="retry — this is a Telegram API issue",
        ) from exc

    # result may be a list or an object with .filters attribute
    filters = result if isinstance(result, list) else getattr(result, "filters", result)

    folders = []
    for f in filters:
        info = _extract_folder_info(f)
        if info is not None:
            folders.append(info)

    if not folders:
        return toon.empty_state(
            "folders",
            "found",
            ["create a folder in Telegram settings first"],
        )

    fields = ["id", "title", "channels"]
    rows = [[f["id"], f["title"], f["channel_count"]] for f in folders]

    return toon.format_response(
        type_name="folders",
        fields=fields,
        rows=rows,
        summary_parts=[f"{len(folders)} folders"],
        next_hints=[
            'Folder contents: tg_execute op="folder_contents" params={"folder": "<title>"}',
            'Create folder: tg_execute op="create_folder" params={"title": "New Folder"}',
        ],
    )


# ---------------------------------------------------------------------------
# folder_contents (T030)
# ---------------------------------------------------------------------------


@operation(
    name="folder_contents",
    category="folders",
    description="List channels in a specific folder by folder title or ID",
    destructive=False,
    idempotent=True,
)
async def folder_contents(
    client: Any,
    folder: str,
    cache: Cache | None = None,
) -> str:
    """List channels in a specific folder."""
    if not folder or not folder.strip():
        raise OperationError(
            what="folder parameter is required",
            expected="folder title or numeric ID",
            example='tg_execute op="folder_contents" params={"folder": "Tech"}',
            recovery="use list_folders to see available folders",
        )

    folder = folder.strip()

    try:
        result = await client(GetDialogFiltersRequest())
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.folder_contents_error")
        raise OperationError(
            what=f"Failed to fetch folders: {type(exc).__name__}: {exc}",
            expected="successful folder fetch",
            example='tg_execute op="folder_contents" params={"folder": "Tech"}',
            recovery="retry — this is a Telegram API issue",
        ) from exc

    filters = result if isinstance(result, list) else getattr(result, "filters", result)

    # Find the target folder by title (case-insensitive) or numeric ID
    target = None
    folder_lower = folder.lower()
    for f in filters:
        info = _extract_folder_info(f)
        if info is None:
            continue
        if info["title"].lower() == folder_lower or str(info["id"]) == folder:
            target = f
            break

    if target is None:
        # Collect available folder names for the error message
        available = []
        for f in filters:
            info = _extract_folder_info(f)
            if info is not None:
                available.append(info["title"])

        raise OperationError(
            what=f"Folder {folder!r} not found",
            expected=f"one of: {', '.join(available)}" if available else "a valid folder title or ID",
            example='tg_execute op="folder_contents" params={"folder": "Tech"}',
            recovery='use tg_execute op="list_folders" to see available folders',
        )

    include_peers = getattr(target, "include_peers", []) or []
    if not include_peers:
        target_info = _extract_folder_info(target)
        title = target_info["title"] if target_info else folder
        return toon.empty_state(
            "channels",
            f"in folder {title!r}",
            ["add channels to this folder via Telegram settings or move_to_folder"],
        )

    # Resolve peer IDs to channel info by iterating dialogs
    peer_ids = set()
    for peer in include_peers:
        pid = _get_peer_id(peer)
        if pid is not None:
            peer_ids.add(pid)

    channels: list[dict[str, Any]] = []
    try:
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if not isinstance(entity, (Channel, Chat)):
                continue
            if entity.id in peer_ids:
                handle = getattr(entity, "username", None)
                is_channel = isinstance(entity, Channel) and entity.broadcast
                channels.append({
                    "title": dialog.name or getattr(entity, "title", ""),
                    "handle": f"@{handle}" if handle else "",
                    "type": "channel" if is_channel else "group",
                    "subscribers": getattr(entity, "participants_count", None) or 0,
                })
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e

    target_info = _extract_folder_info(target)
    title = target_info["title"] if target_info else folder

    if not channels:
        return toon.empty_state(
            "channels",
            f"resolvable in folder {title!r}",
            ["some peers may be users or inaccessible channels"],
        )

    channels.sort(key=lambda c: c["title"].lower())

    fields = ["title", "handle", "type", "subscribers"]
    rows = [[c["title"], c["handle"], c["type"], c["subscribers"]] for c in channels]

    return toon.format_response(
        type_name="channels",
        fields=fields,
        rows=rows,
        summary_parts=[f"{len(channels)} channels in {title!r}", f"{len(peer_ids)} total peers"],
        next_hints=[
            'Channel info: tg_execute op="channel_info" params={"channel": "@handle"}',
            'Move channel: tg_execute op="move_to_folder" params={"channel": "@handle", "folder": "Other Folder"}',
        ],
    )


# ---------------------------------------------------------------------------
# move_to_folder (T031)
# ---------------------------------------------------------------------------


@operation(
    name="move_to_folder",
    category="folders",
    description="Move a channel into a folder. Adds the channel to the target folder's include_peers",
    destructive=False,
    idempotent=True,
)
async def move_to_folder(
    client: Any,
    channel: str,
    folder: str,
    cache: Cache | None = None,
) -> str:
    """Move a channel into a folder."""
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="move_to_folder" params={"channel": "@llm_under_hood", "folder": "Tech"}',
            recovery="provide a channel identifier",
        )

    if not folder or not folder.strip():
        raise OperationError(
            what="folder parameter is required",
            expected="folder title or numeric ID",
            example='tg_execute op="move_to_folder" params={"channel": "@handle", "folder": "Tech"}',
            recovery='use tg_execute op="list_folders" to see available folders',
        )

    channel = channel.strip()
    folder = folder.strip()

    entity = await _resolve_single_channel(client, channel)

    # Fetch folders
    try:
        result = await client(GetDialogFiltersRequest())
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.move_to_folder_fetch_error")
        raise OperationError(
            what=f"Failed to fetch folders: {type(exc).__name__}: {exc}",
            expected="successful folder fetch",
            example='tg_execute op="move_to_folder" params={"channel": "@handle", "folder": "Tech"}',
            recovery="retry — this is a Telegram API issue",
        ) from exc

    filters = result if isinstance(result, list) else getattr(result, "filters", result)

    # Find target folder
    target_filter = None
    folder_lower = folder.lower()
    for f in filters:
        info = _extract_folder_info(f)
        if info is None:
            continue
        if info["title"].lower() == folder_lower or str(info["id"]) == folder:
            target_filter = f
            break

    if target_filter is None:
        available = []
        for f in filters:
            info = _extract_folder_info(f)
            if info is not None:
                available.append(info["title"])
        raise OperationError(
            what=f"Folder {folder!r} not found",
            expected=f"one of: {', '.join(available)}" if available else "a valid folder title or ID",
            example='tg_execute op="move_to_folder" params={"channel": "@handle", "folder": "Tech"}',
            recovery='use tg_execute op="list_folders" to see available folders',
        )

    # Build InputPeerChannel for the entity
    from telethon.tl.types import InputPeerChannel, InputPeerChat

    if isinstance(entity, Channel):
        input_peer = InputPeerChannel(entity.id, entity.access_hash or 0)
    elif isinstance(entity, Chat):
        input_peer = InputPeerChat(entity.id)
    else:
        raise OperationError(
            what=f"Cannot build peer for {type(entity).__name__}",
            expected="Channel or Chat entity",
            example='tg_execute op="move_to_folder" params={"channel": "@handle", "folder": "Tech"}',
            recovery="use a channel or group, not a user",
        )

    # Check if already in folder
    include_peers = list(getattr(target_filter, "include_peers", []) or [])
    existing_ids = set()
    for peer in include_peers:
        pid = _get_peer_id(peer)
        if pid is not None:
            existing_ids.add(pid)

    target_info = _extract_folder_info(target_filter)
    title = target_info["title"] if target_info else folder

    if entity.id in existing_ids:
        handle = getattr(entity, "username", None)
        handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)
        return f"{handle_display} is already in folder {title!r}."

    # Add to include_peers and update
    include_peers.append(input_peer)
    target_filter.include_peers = include_peers

    try:
        await client(UpdateDialogFilterRequest(
            id=target_filter.id,
            filter=target_filter,
        ))
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.move_to_folder_update_error")
        raise OperationError(
            what=f"Failed to update folder: {type(exc).__name__}: {exc}",
            expected="successful folder update",
            example='tg_execute op="move_to_folder" params={"channel": "@handle", "folder": "Tech"}',
            recovery="retry — the folder structure may have changed",
        ) from exc

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)

    return f"Moved {handle_display} to folder {title!r}."


# ---------------------------------------------------------------------------
# remove_from_folder
# ---------------------------------------------------------------------------


@operation(
    name="remove_from_folder",
    category="folders",
    description="Remove a channel from a folder without unsubscribing",
    destructive=False,
    idempotent=True,
)
async def remove_from_folder(
    client: Any,
    channel: str,
    folder: str,
    cache: Cache | None = None,
) -> str:
    """Remove a channel from a folder."""
    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="remove_from_folder" params={"channel": "@handle", "folder": "Tech"}',
            recovery="provide a channel identifier",
        )
    if not folder or not folder.strip():
        raise OperationError(
            what="folder parameter is required",
            expected="folder title or numeric ID",
            example='tg_execute op="remove_from_folder" params={"channel": "@handle", "folder": "Tech"}',
            recovery='use tg_execute op="list_folders" to see folders',
        )

    channel = channel.strip()
    folder = folder.strip()

    entity = await _resolve_single_channel(client, channel)

    try:
        result = await client(GetDialogFiltersRequest())
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e

    filters = result if isinstance(result, list) else getattr(result, "filters", result)

    target_filter = None
    for f in filters:
        info = _extract_folder_info(f)
        if info is None:
            continue
        if info["title"].lower() == folder.lower() or str(info["id"]) == folder:
            target_filter = f
            break

    if target_filter is None:
        raise OperationError(
            what=f"Folder {folder!r} not found",
            expected="existing folder title or ID",
            example='tg_execute op="remove_from_folder" params={"channel": "@handle", "folder": "Tech"}',
            recovery='use tg_execute op="list_folders" to see folders',
        )

    include_peers = list(getattr(target_filter, "include_peers", []) or [])
    new_peers = [p for p in include_peers if _get_peer_id(p) != entity.id]

    target_info = _extract_folder_info(target_filter)
    title = target_info["title"] if target_info else folder

    if len(new_peers) == len(include_peers):
        handle = getattr(entity, "username", None)
        handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)
        return f"{handle_display} is not in folder {title!r}."

    target_filter.include_peers = new_peers

    # Also remove from pinned_peers if present
    pinned = list(getattr(target_filter, "pinned_peers", []) or [])
    target_filter.pinned_peers = [p for p in pinned if _get_peer_id(p) != entity.id]

    try:
        await client(UpdateDialogFilterRequest(
            id=target_filter.id,
            filter=target_filter,
        ))
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.remove_from_folder_error")
        raise OperationError(
            what=f"Failed to update folder: {type(exc).__name__}: {exc}",
            expected="successful folder update",
            example='tg_execute op="remove_from_folder" params={"channel": "@handle", "folder": "Tech"}',
            recovery="retry",
        ) from exc

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)

    return f"Removed {handle_display} from folder {title!r}."


# ---------------------------------------------------------------------------
# create_folder (T031)
# ---------------------------------------------------------------------------


@operation(
    name="create_folder",
    category="folders",
    description="Create a new Telegram folder with an initial channel (Telegram requires at least one)",
    destructive=False,
    idempotent=False,
)
async def create_folder(
    client: Any,
    title: str,
    channel: str,
    cache: Cache | None = None,
) -> str:
    """Create a new Telegram folder with an initial channel."""
    if not title or not title.strip():
        raise OperationError(
            what="title parameter is required",
            expected="non-empty folder title",
            example='tg_execute op="create_folder" params={"title": "AI", "channel": "@llm_under_hood"}',
            recovery="provide a folder name",
        )

    if not channel or not channel.strip():
        raise OperationError(
            what="channel parameter is required (Telegram requires at least one channel per folder)",
            expected="@handle, t.me link, or channel title",
            example='tg_execute op="create_folder" params={"title": "AI", "channel": "@llm_under_hood"}',
            recovery="provide a channel to seed the folder with",
        )

    title = title.strip()
    channel = channel.strip()

    if len(title) > 12:
        raise OperationError(
            what=f"Folder title too long ({len(title)} chars). Telegram allows max 12",
            expected="title with 1-12 characters",
            example='tg_execute op="create_folder" params={"title": "AI", "channel": "@handle"}',
            recovery="shorten the title",
        )

    # Resolve the seed channel
    entity = await _resolve_single_channel(client, channel)

    # Build InputPeer for the seed channel
    from telethon.tl.types import InputPeerChannel, InputPeerChat

    if isinstance(entity, Channel):
        input_peer = InputPeerChannel(entity.id, getattr(entity, "access_hash", 0) or 0)
    elif isinstance(entity, Chat):
        input_peer = InputPeerChat(entity.id)
    else:
        raise OperationError(
            what=f"Cannot add {type(entity).__name__} to folder",
            expected="Channel or Chat entity",
            example='tg_execute op="create_folder" params={"title": "AI", "channel": "@handle"}',
            recovery="use a channel or group, not a user",
        )

    # Fetch existing folders to check for duplicates and find next free ID
    try:
        result = await client(GetDialogFiltersRequest())
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.create_folder_fetch_error")
        raise OperationError(
            what=f"Failed to fetch folders: {type(exc).__name__}: {exc}",
            expected="successful folder fetch",
            example='tg_execute op="create_folder" params={"title": "AI", "channel": "@handle"}',
            recovery="retry — this is a Telegram API issue",
        ) from exc

    filters = result if isinstance(result, list) else getattr(result, "filters", result)

    # Check for duplicate title
    title_lower = title.lower()
    used_ids = set()
    for f in filters:
        info = _extract_folder_info(f)
        if info is None:
            continue
        used_ids.add(info["id"])
        if info["title"].lower() == title_lower:
            raise OperationError(
                what=f"Folder {title!r} already exists (id={info['id']})",
                expected="unique folder title",
                example='tg_execute op="create_folder" params={"title": "Different", "channel": "@handle"}',
                recovery="choose a different title or use the existing folder",
            )

    # Find next available filter ID (Telegram uses 2-255 for custom filters)
    new_id = 2
    while new_id in used_ids and new_id < 256:
        new_id += 1

    if new_id >= 256:
        raise OperationError(
            what="Maximum number of folders reached",
            expected="fewer than 254 custom folders",
            example='tg_execute op="list_folders"',
            recovery="delete an existing folder first",
        )

    # Create the filter with the seed channel
    from telethon.tl.types import DialogFilter, TextWithEntities

    new_filter = DialogFilter(
        id=new_id,
        title=TextWithEntities(text=title, entities=[]),
        pinned_peers=[],
        include_peers=[input_peer],
        exclude_peers=[],
    )

    try:
        await client(UpdateDialogFilterRequest(
            id=new_id,
            filter=new_filter,
        ))
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.create_folder_error")
        raise OperationError(
            what=f"Failed to create folder: {type(exc).__name__}: {exc}",
            expected="successful folder creation",
            example='tg_execute op="create_folder" params={"title": "AI", "channel": "@handle"}',
            recovery="retry — Telegram may have a temporary issue",
        ) from exc

    handle = getattr(entity, "username", None)
    handle_display = f"@{handle}" if handle else getattr(entity, "title", channel)

    lines = [
        f"Created folder {title!r} (id={new_id}) with {handle_display}.",
        "",
        toon.hint(f'Add more: tg_execute op="move_to_folder" params={{"channel": "@handle", "folder": "{title}"}}'),
        toon.hint('List folders: tg_execute op="list_folders"'),
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# rename_folder
# ---------------------------------------------------------------------------


@operation(
    name="rename_folder",
    category="folders",
    description="Rename an existing Telegram folder",
    destructive=False,
    idempotent=True,
)
async def rename_folder(
    client: Any,
    folder: str,
    new_title: str,
    cache: Cache | None = None,
) -> str:
    """Rename a folder."""
    if not folder or not folder.strip():
        raise OperationError(
            what="folder parameter is required",
            expected="current folder title or numeric ID",
            example='tg_execute op="rename_folder" params={"folder": "UA", "new_title": "News"}',
            recovery='use tg_execute op="list_folders" to see folders',
        )
    if not new_title or not new_title.strip():
        raise OperationError(
            what="new_title parameter is required",
            expected="non-empty new folder title",
            example='tg_execute op="rename_folder" params={"folder": "UA", "new_title": "News"}',
            recovery="provide the new name",
        )

    folder = folder.strip()
    new_title = new_title.strip()

    if len(new_title) > 12:
        raise OperationError(
            what=f"New title too long ({len(new_title)} chars). Max 12",
            expected="1-12 characters",
            example='tg_execute op="rename_folder" params={"folder": "UA", "new_title": "News"}',
            recovery="shorten the title",
        )

    try:
        result = await client(GetDialogFiltersRequest())
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e

    filters = result if isinstance(result, list) else getattr(result, "filters", result)

    # Find target filter
    target_filter = None
    for f in filters:
        info = _extract_folder_info(f)
        if info is None:
            continue
        if info["title"].lower() == folder.lower() or str(info["id"]) == folder:
            target_filter = f
            break

    if target_filter is None:
        raise OperationError(
            what=f"Folder {folder!r} not found",
            expected="existing folder title or ID",
            example='tg_execute op="rename_folder" params={"folder": "UA", "new_title": "News"}',
            recovery='use tg_execute op="list_folders" to see folders',
        )

    # Update title
    from telethon.tl.types import TextWithEntities

    old_title = _extract_folder_info(target_filter)["title"]

    if hasattr(target_filter, "title"):
        target_filter.title = TextWithEntities(text=new_title, entities=[])

    try:
        await client(UpdateDialogFilterRequest(
            id=target_filter.id,
            filter=target_filter,
        ))
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.rename_folder_error")
        raise OperationError(
            what=f"Failed to rename folder: {type(exc).__name__}: {exc}",
            expected="successful rename",
            example='tg_execute op="rename_folder" params={"folder": "UA", "new_title": "News"}',
            recovery="retry",
        ) from exc

    return f"Renamed folder {old_title!r} -> {new_title!r}."


# ---------------------------------------------------------------------------
# reorder_folders
# ---------------------------------------------------------------------------


@operation(
    name="reorder_folders",
    category="folders",
    description="Reorder Telegram folders by providing a list of folder titles in desired order",
    destructive=False,
    idempotent=True,
)
async def reorder_folders(
    client: Any,
    order: list[str],
    cache: Cache | None = None,
) -> str:
    """Reorder folders. Accepts a list of folder titles in desired order."""
    if not order:
        raise OperationError(
            what="order parameter is required",
            expected="list of folder titles in desired order",
            example='tg_execute op="reorder_folders" params={"order": ["Fav", "AI", "Dev"]}',
            recovery='use tg_execute op="list_folders" to see current folders',
        )

    try:
        result = await client(GetDialogFiltersRequest())
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e

    filters = result if isinstance(result, list) else getattr(result, "filters", result)

    # Build title -> id mapping
    title_to_id: dict[str, int] = {}
    all_ids: list[int] = []
    for f in filters:
        info = _extract_folder_info(f)
        if info is None:
            # Default/system filters — keep their position
            fid = getattr(f, "id", None)
            if fid is not None:
                all_ids.append(fid)
            continue
        title_to_id[info["title"].lower()] = info["id"]
        all_ids.append(info["id"])

    # Build ordered ID list
    ordered_ids: list[int] = []
    for title in order:
        title_lower = title.strip().lower()
        fid = title_to_id.get(title_lower)
        if fid is None:
            raise OperationError(
                what=f"Folder {title!r} not found",
                expected=f"one of: {', '.join(sorted(title_to_id.keys()))}",
                example='tg_execute op="reorder_folders" params={"order": ["Fav", "AI"]}',
                recovery='use tg_execute op="list_folders" to see folder names',
            )
        ordered_ids.append(fid)

    # Append any folders not mentioned in order (keep them at the end)
    for fid in all_ids:
        if fid not in ordered_ids:
            ordered_ids.append(fid)

    from telethon.tl.functions.messages import UpdateDialogFiltersOrderRequest

    try:
        await client(UpdateDialogFiltersOrderRequest(order=ordered_ids))
    except FloodWaitError as e:
        raise TelegramFloodWait(e.seconds) from e
    except Exception as exc:
        logger.exception("ops.reorder_folders_error")
        raise OperationError(
            what=f"Failed to reorder: {type(exc).__name__}: {exc}",
            expected="successful reorder",
            example='tg_execute op="reorder_folders" params={"order": ["Fav", "AI"]}',
            recovery="retry",
        ) from exc

    return f"Folders reordered: {' → '.join(order)}."
