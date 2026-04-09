"""One-time Telegram authentication CLI.

Invoked via: python -m tg_mcp.auth

Creates a Telethon session file at ~/.tg-mcp/session.session.
Uses Telethon's built-in interactive auth flow (phone code + optional 2FA).
"""

from __future__ import annotations

import asyncio
import os
import sys

from telethon import TelegramClient

from tg_mcp.config import ConfigError, load_settings


async def _authenticate() -> None:
    """Run the interactive Telegram authentication flow."""
    print("Telegram MCP — First-time authentication\n")

    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"Configuration error:\n{exc}", file=sys.stderr)
        sys.exit(1)

    # Telethon appends .session to the path, so strip the suffix
    session_str = str(settings.session_path.with_suffix(""))

    client = TelegramClient(
        session_str,
        api_id=settings.api_id,
        api_hash=settings.api_hash,
    )

    try:
        # Telethon's start() handles the full interactive flow:
        # - Sends code to phone
        # - Prompts for code via input()
        # - Prompts for 2FA password if enabled
        await client.start(phone=settings.phone)

        me = await client.get_me()
        display = me.first_name or me.username or str(me.id)
        print(f"\nAuthenticated as: {display}")

    except Exception as exc:
        print(f"\nAuthentication failed: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await client.disconnect()

    # Lock down session file permissions
    if settings.session_path.exists():
        os.chmod(settings.session_path, 0o600)

    print(f"Session saved: {settings.session_path}")
    print("\nSetup complete. Start the MCP server with: python -m tg_mcp")


def main() -> None:
    """Entry point for python -m tg_mcp.auth."""
    asyncio.run(_authenticate())


if __name__ == "__main__":
    main()
