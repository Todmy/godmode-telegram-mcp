# Telegram MCP Server

MCP server that exposes full Telegram client capabilities to AI assistants (Claude Code, Perplexity, and other MCP-compatible clients). Uses Telethon (User API / MTProto) for channel reading, message search, reactions, folder management, and analytics across your subscribed channels.

## Prerequisites

- macOS (Apple Silicon or Intel)
- Python 3.11+
- Telegram API credentials (`api_id` and `api_hash` from [my.telegram.org](https://my.telegram.org))
- Your phone number registered with Telegram

## Setup

### 1. Install Python 3.11+ (skip if already installed)

Open **Terminal** (press `Cmd + Space`, type "Terminal", press Enter).

Check if Python 3.11+ is already installed:

```bash
python3.11 --version
```

If you see `Python 3.11.x` or higher — skip to step 2. If you see "command not found":

```bash
# Install Homebrew (macOS package manager) if you don't have it:
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Python 3.11:
brew install python@3.11
```

After installation, note the path — you'll need it later:

```bash
which python3.11
```

This will show something like `/opt/homebrew/bin/python3.11` (Apple Silicon) or `/usr/local/bin/python3.11` (Intel Mac). **Save this path.**

### 2. Clone and install

```bash
git clone https://github.com/Todmy/telegram-connector-mcp.git
cd telegram-connector-mcp
python3.11 -m pip install -e .
```

### 3. Get Telegram API credentials

1. Open [my.telegram.org](https://my.telegram.org) in your browser
2. Log in with your phone number (international format, e.g. `+380501234567`)
3. Telegram will send a code to your Telegram app — enter it on the website
4. Click **"API development tools"**
5. Fill in the form:
   - **App title:** anything (e.g. "My MCP")
   - **Short name:** anything (e.g. "mymcp")
   - **Platform:** can leave default
6. Click **Create application**
7. You will see **App api_id** (a number) and **App api_hash** (a long hex string) — copy both

### 4. Configure

Back in Terminal, create the config:

```bash
mkdir -p ~/.tg-mcp
cp .env.example ~/.tg-mcp/.env
```

Open the config file in a text editor:

```bash
open -e ~/.tg-mcp/.env
```

Fill in your credentials:

```env
TG_API_ID=12345678
TG_API_HASH=0123456789abcdef0123456789abcdef
TG_PHONE=+380501234567
```

Replace with your actual values from step 3. Save and close the file.

### 5. Authenticate with Telegram

This is a one-time step. Telegram will send a verification code to your phone:

```bash
python3.11 -m tg_mcp.auth
```

- Enter the code from Telegram when prompted
- If you have **two-factor authentication (2FA)** enabled, you'll also be asked for your password

After successful auth, a session file is created at `~/.tg-mcp/session.session`. You won't need to authenticate again unless you revoke the session.

### 6. Connect to your AI client

<details>
<summary><strong>Perplexity (Mac app)</strong></summary>

> Requires the [Perplexity Mac app](https://apps.apple.com/app/perplexity-ask-anything/id6714467650) from the App Store.

#### 6a. Find your Python path

In Terminal, run:

```bash
which python3.11
```

Copy the full path (e.g. `/opt/homebrew/bin/python3.11`).

> **Why this matters:** Perplexity runs in a macOS sandbox and may find a different Python than the one you installed packages to. Using the full path ensures it finds the right one.

#### 6b. Add the connector in Perplexity

1. Open **Perplexity** → click your avatar (bottom-left) → **Settings**
2. Go to **MCP** (or **Connectors**) in the sidebar
3. If prompted to install **PerplexityXPC** helper — click Install and follow the instructions. This is required for Perplexity to run local programs.
4. Click **"Add"** (or **"+"**)
5. Stay on the **Simple** tab
6. Fill in:
   - **Server Name:** `telegram-mcp`
   - **Command:** your full Python path + `-m tg_mcp`, for example:
     ```
     /opt/homebrew/bin/python3.11 -m tg_mcp
     ```
   - **Environment Variables:** leave empty (credentials are loaded from `~/.tg-mcp/.env` automatically)
7. Click **Save**
8. Wait a few seconds — the status should change to **Running** (green)

#### 6c. Enable the connector

Go back to the Perplexity home screen. Under the search bar, click **Sources** and enable **telegram-mcp**.

Now you can ask Perplexity things like:
- "Show me recent messages from @channel_name"
- "Search my Telegram channels for AI governance"
- "Which of my channels had the most engagement this week?"

#### Troubleshooting Perplexity

| Error | Cause | Fix |
|---|---|---|
| `No module named tg_mcp` | Wrong Python path | Run `which python3.11` in Terminal and use the full path in the Command field |
| `TimedOutError` | Command is incomplete | Make sure the command includes `-m tg_mcp` after the Python path |
| `The operation couldn't be completed` | PerplexityXPC not installed | Go to Settings → MCP and install the PerplexityXPC helper |
| Connector stuck on "Starting" | First connection to Telegram | Be patient — the first run may take 10-15 seconds to connect |

</details>

<details>
<summary><strong>Claude Code (CLI)</strong></summary>

Add to your Claude Code MCP settings (`~/.claude/settings.json`):

```json
{
  "mcpServers": {
    "telegram-mcp": {
      "command": "python3.11",
      "args": ["-m", "tg_mcp"]
    }
  }
}
```

</details>

<details>
<summary><strong>Other MCP clients (Cursor, Windsurf, etc.)</strong></summary>

Any MCP client that supports **stdio** transport can use this server. The command to run:

```bash
python3.11 -m tg_mcp
```

Refer to your client's documentation for how to register an MCP server with a shell command.

</details>

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
