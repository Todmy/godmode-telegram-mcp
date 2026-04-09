"""Telethon client wrapper — lazy connect, auto-reconnect, FloodWait handling.

The client connects on first actual use, not on import or MCP server startup.
Session file is stored at ~/.tg-mcp/session.session.
"""

from __future__ import annotations

import asyncio
import os
import re
import stat
from pathlib import Path

from telethon import TelegramClient as _TelethonClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.types import Channel, Chat, User

from tg_mcp.config import Settings, logger


class TelegramConnectionError(Exception):
    """Raised when Telegram connection cannot be established."""


class TelegramFloodWait(Exception):
    """Raised when Telegram rate-limits us. Contains wait duration."""

    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
        super().__init__(
            f"Rate limited by Telegram. Retry in {seconds}s. "
            f"This is enforced server-side and cannot be bypassed."
        )


class ChannelResolutionError(Exception):
    """Raised when a channel identifier cannot be resolved to a Telegram entity."""


class TelegramClient:
    """Lazy-connecting Telethon wrapper with defensive error handling.

    - Connects on first get() call, not on construction.
    - Auto-reconnect is handled by Telethon internally.
    - FloodWaitError: waits the required time, then raises TelegramFloodWait.
    - 30s timeout on connection.
    - Validates session file permissions (should be 600).
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: _TelethonClient | None = None
        self._connect_lock = asyncio.Lock()
        self._connected = False

    @property
    def session_path(self) -> Path:
        return self._settings.session_path

    def _check_session_file(self) -> None:
        """Validate session file exists and has safe permissions."""
        path = self.session_path
        if not path.exists():
            raise TelegramConnectionError(
                f"Session file not found at {path}\n"
                f"Expected: Telethon session file from prior authentication\n"
                f"Example: python -m tg_mcp.auth\n"
                f"Recovery: run the auth command to create a session"
            )

        try:
            file_stat = os.stat(path)
            mode = stat.S_IMODE(file_stat.st_mode)
            if mode & (stat.S_IRGRP | stat.S_IROTH | stat.S_IWGRP | stat.S_IWOTH):
                logger.warning(
                    "client.session_permissions_unsafe",
                    extra={"path": str(path), "mode": oct(mode)},
                )
                try:
                    os.chmod(path, 0o600)
                    logger.info("client.session_permissions_fixed", extra={"path": str(path)})
                except OSError:
                    pass
        except OSError:
            pass

    async def get(self) -> _TelethonClient:
        """Get a connected Telethon client. Connects lazily on first call."""
        if self._connected and self._client is not None:
            if self._client.is_connected():
                return self._client
            logger.warning("client.connection_dropped")
            self._connected = False

        async with self._connect_lock:
            if self._connected and self._client is not None and self._client.is_connected():
                return self._client

            return await self._connect()

    async def _connect(self) -> _TelethonClient:
        """Establish connection to Telegram. Internal — called under lock."""
        self._check_session_file()

        session_str = str(self.session_path.with_suffix(""))

        self._client = _TelethonClient(
            session_str,
            api_id=self._settings.api_id,
            api_hash=self._settings.api_hash,
            timeout=30,
            auto_reconnect=True,
        )

        try:
            logger.info("client.connecting")
            await asyncio.wait_for(self._client.connect(), timeout=30.0)

            if not await self._client.is_user_authorized():
                raise TelegramConnectionError(
                    "Session exists but is not authorized.\n"
                    "Expected: authorized Telethon session\n"
                    "Example: python -m tg_mcp.auth\n"
                    "Recovery: re-run the auth command — session may have expired"
                )

            self._connected = True
            logger.info("client.connected")
            return self._client

        except FloodWaitError as e:
            logger.warning(
                "client.flood_wait_on_connect",
                extra={"wait_seconds": e.seconds},
            )
            await asyncio.sleep(e.seconds)
            raise TelegramFloodWait(e.seconds) from e

        except asyncio.TimeoutError:
            raise TelegramConnectionError(
                "Connection to Telegram timed out after 30s.\n"
                "Expected: successful MTProto connection\n"
                "Example: check network connectivity\n"
                "Recovery: retry in a few seconds — Telegram may be temporarily unreachable"
            )
        except (TelegramConnectionError, TelegramFloodWait):
            raise
        except Exception as exc:
            raise TelegramConnectionError(
                f"Failed to connect to Telegram: {exc}\n"
                f"Expected: successful connection with valid session\n"
                f"Example: python -m tg_mcp.auth to re-authenticate\n"
                f"Recovery: check network, verify session file, re-auth if needed"
            ) from exc

    # ------------------------------------------------------------------
    # Channel resolution
    # ------------------------------------------------------------------

    # Valid @handle pattern: 5-32 alphanumeric + underscores, cannot start/end with _
    _HANDLE_RE = re.compile(r"^@?([a-zA-Z][a-zA-Z0-9_]{3,30}[a-zA-Z0-9])$")

    # t.me link patterns (including joinchat and +invite links)
    _TGLINK_RE = re.compile(
        r"^https?://(?:t\.me|telegram\.me)/(?:\+|joinchat/)?([a-zA-Z0-9_]+)$"
    )

    async def resolve_channel(
        self, identifier: str
    ) -> list[Channel | Chat]:
        """Resolve a channel identifier to one or more Telegram entities.

        Accepts:
            - @handle (e.g. @llm_under_hood)
            - t.me link (e.g. https://t.me/llm_under_hood)
            - title substring (fuzzy match against subscribed channels)

        Returns a list of matching Channel/Chat entities. For @handle and
        t.me links this will be exactly one entity; for title substrings it
        may be multiple.

        Raises:
            ChannelResolutionError: if no matching entity is found or access denied.
            TelegramFloodWait: if rate-limited during resolution.
        """
        if not identifier or not identifier.strip():
            raise ChannelResolutionError(
                "Channel identifier is empty. "
                "Provide a @handle, t.me link, or channel title substring."
            )

        identifier = identifier.strip()

        # Try t.me link first (before handle check, as links contain handles)
        link_match = self._TGLINK_RE.match(identifier)
        if link_match:
            username = link_match.group(1)
            return [await self._resolve_by_handle(username)]

        # Try @handle (with or without @ prefix)
        handle_match = self._HANDLE_RE.match(identifier)
        if handle_match:
            username = handle_match.group(1)
            return [await self._resolve_by_handle(username)]

        # If identifier starts with @ but doesn't match handle format, fail explicitly
        if identifier.startswith("@"):
            raise ChannelResolutionError(
                f"Invalid handle format: {identifier!r}. "
                f"Handles must be 5-32 characters, alphanumeric and underscores only, "
                f"starting with a letter. Example: @llm_under_hood"
            )

        # Fall back to title substring match against subscribed channels
        return await self._resolve_by_title(identifier)

    async def _resolve_by_handle(self, username: str) -> Channel | Chat:
        """Resolve a single channel by username. Raises on failure."""
        tg = await self.get()

        try:
            entity = await tg.get_entity(username)
        except UsernameNotOccupiedError:
            raise ChannelResolutionError(
                f"Channel @{username} does not exist. "
                f"Check the handle spelling. "
                f"Use tg_overview to see your subscribed channels."
            )
        except UsernameInvalidError:
            raise ChannelResolutionError(
                f"Invalid username: @{username}. "
                f"Telegram rejected this as a malformed handle."
            )
        except ChannelPrivateError:
            raise ChannelResolutionError(
                f"Channel @{username} is private or you were banned. "
                f"You need to be a member to access this channel."
            )
        except FloodWaitError as e:
            logger.warning(
                "client.flood_wait_on_resolve",
                extra={"username": username, "wait_seconds": e.seconds},
            )
            raise TelegramFloodWait(e.seconds) from e
        except Exception as exc:
            raise ChannelResolutionError(
                f"Failed to resolve @{username}: {type(exc).__name__}: {exc}. "
                f"Check the handle and try again."
            ) from exc

        if isinstance(entity, User):
            raise ChannelResolutionError(
                f"@{username} is a user account, not a channel or group. "
                f"tg_feed only works with channels and groups."
            )

        if not isinstance(entity, (Channel, Chat)):
            raise ChannelResolutionError(
                f"@{username} resolved to an unexpected type: {type(entity).__name__}. "
                f"Expected a channel or group."
            )

        return entity

    async def _resolve_by_title(
        self, substring: str
    ) -> list[Channel | Chat]:
        """Resolve channels by title substring match.

        Searches all subscribed dialogs. Returns all matches.
        Raises ChannelResolutionError if none found.
        """
        tg = await self.get()
        substring_lower = substring.lower()
        matches: list[Channel | Chat] = []

        try:
            async for dialog in tg.iter_dialogs():
                entity = dialog.entity
                if not isinstance(entity, (Channel, Chat)):
                    continue
                if substring_lower in dialog.name.lower():
                    matches.append(entity)
        except FloodWaitError as e:
            logger.warning(
                "client.flood_wait_on_title_search",
                extra={"substring": substring, "wait_seconds": e.seconds},
            )
            raise TelegramFloodWait(e.seconds) from e
        except Exception as exc:
            raise ChannelResolutionError(
                f"Error searching channels by title {substring!r}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        if not matches:
            raise ChannelResolutionError(
                f"No subscribed channel matches {substring!r}. "
                f"Check the spelling or use tg_overview to see all channels."
            )

        return matches

    async def disconnect(self) -> None:
        """Gracefully disconnect from Telegram."""
        self._connected = False
        if self._client is not None:
            try:
                await self._client.disconnect()
                logger.info("client.disconnected")
            except Exception:
                logger.exception("client.disconnect_error")
            finally:
                self._client = None

    async def __aenter__(self) -> TelegramClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.disconnect()
