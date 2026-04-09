# Telegram MCP Server

MCP server that exposes full Telegram client capabilities to Claude Code. Uses Telethon (User API / MTProto) for channel reading, message search, reactions, folder management, and analytics across your subscribed channels.

## Prerequisites

- Python 3.11+
- Telegram API credentials (`api_id` and `api_hash` from [my.telegram.org](https://my.telegram.org))
- Your phone number registered with Telegram

## Setup

### 1. Install

```bash
git clone https://github.com/todmy/telegram-mcp.git
cd telegram-mcp
pip install -e .
```

### 2. Configure

Create the config directory and `.env` file:

```bash
mkdir -p ~/.tg-mcp
cp .env.example ~/.tg-mcp/.env
```

Edit `~/.tg-mcp/.env` with your credentials:

```env
TG_API_ID=12345678
TG_API_HASH=0123456789abcdef0123456789abcdef
TG_PHONE=+380501234567
```

### 3. Authenticate

Run the one-time auth flow. Telegram will send a code to your phone:

```bash
python -m tg_mcp.auth
```

Enter the code when prompted. If you have 2FA enabled, you'll be asked for your password too.

### 4. Register with Claude Code

Add to your Claude Code MCP settings (`~/.claude/settings.json`):

```json
{
  "mcpServers": {
    "telegram": {
      "command": "python",
      "args": ["-m", "tg_mcp"],
      "cwd": "/path/to/telegram-mcp"
    }
  }
}
```

## Usage

The server exposes 5 MCP tools. Two are shortcuts for common tasks, three implement the dynamic toolsets pattern for 23 operations.

### Quick examples

**Read recent messages from a channel:**
```
tg_feed channel="@llm_under_hood" hours=24
```

**List all subscribed channels sorted by activity:**
```
tg_overview sort=activity
```

**Find an operation by keyword:**
```
tg_search_ops query="react"
```

**Get operation details:**
```
tg_describe_op name="react_to_message"
```

**Execute an operation:**
```
tg_execute op="search_messages" params={"query": "AI governance", "hours": 168}
```

## MCP Tools

| Tool | Purpose |
|---|---|
| `tg_feed` | Read channel messages with time window, field selection, truncation |
| `tg_overview` | Channel/folder overview with sorting, filtering, metrics |
| `tg_search_ops` | Search the operations catalog by keyword or category |
| `tg_describe_op` | Get full schema and usage for a specific operation |
| `tg_execute` | Execute any operation with parameter validation |

## Available Operations (23)

### Channels
| Operation | Description |
|---|---|
| `list_channels` | List all subscribed channels and groups with basic info |
| `channel_info` | Detailed info: description, admins, creation date, subscribers |
| `channel_stats` | Activity stats: post frequency, avg views, engagement rate |
| `subscribe` | Join a channel by @handle or t.me link |
| `unsubscribe` | Leave a channel (destructive, requires `confirm=true`) |
| `mute_channel` | Mute or unmute channel notifications |

### Messages
| Operation | Description |
|---|---|
| `search_messages` | Keyword search across all or specific channels |
| `get_message` | Fetch single message with full content and media metadata |
| `message_history` | Paginated message history for a channel |
| `who_posted_first` | Find which channel posted about a topic first |

### Interactions
| Operation | Description |
|---|---|
| `react_to_message` | Add emoji reaction to a message |
| `send_comment` | Post comment in channel discussion thread |
| `forward_message` | Forward message to Saved Messages or specified chat |
| `mark_read` | Mark all messages in a channel as read |

### Folders
| Operation | Description |
|---|---|
| `list_folders` | List all Telegram folders with channel counts |
| `folder_contents` | List channels in a specific folder |
| `move_to_folder` | Move a channel into a folder |
| `create_folder` | Create a new empty folder |

### Analytics
| Operation | Description |
|---|---|
| `compare_channels` | Side-by-side metrics for 2+ channels |
| `find_duplicates` | Detect cross-posted content by text similarity |
| `inactive_channels` | Find channels with no posts in N days |
| `top_posts` | Highest-engagement messages across channels |
| `engagement_ranking` | Rank channels by engagement rate |

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `ConfigError: .env file not found` | Missing config file | Run `cp .env.example ~/.tg-mcp/.env` and fill in credentials |
| `ConfigError: TG_API_ID is missing` | Empty or missing API ID | Get credentials from [my.telegram.org](https://my.telegram.org) |
| `Session file not found` | Auth not completed | Run `python -m tg_mcp.auth` |
| `Session exists but is not authorized` | Expired or revoked session | Re-run `python -m tg_mcp.auth` |
| `Connection timed out after 30s` | Network issue or Telegram down | Check internet connectivity, retry |
| `Rate limited by Telegram` | Too many requests | Wait the indicated time, then retry |
| `Permission denied on session file` | File permissions too open | Run `chmod 600 ~/.tg-mcp/session.session` |

## Architecture

The server uses the **Speakeasy Dynamic Toolsets** pattern: 5 static MCP tools keep the tool list small (token-efficient), while 23 operations are discoverable through `tg_search_ops` / `tg_describe_op` / `tg_execute`. List responses use the TOON format for 30-60% token reduction vs JSON.

## Data Locations

| Path | Contents |
|---|---|
| `~/.tg-mcp/.env` | API credentials |
| `~/.tg-mcp/session.session` | Telethon session (auth state) |
| `~/.tg-mcp/tg_mcp.db` | SQLite cache (channels, messages) |
| `~/.tg-mcp/logs/` | Structured JSON logs |

## License

Private.
