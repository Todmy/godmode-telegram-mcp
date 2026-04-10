# Quickstart: Godmode Telegram MCP

## Prerequisites

- Python 3.11+
- Telegram account with API credentials from [my.telegram.org](https://my.telegram.org)
- Claude Code installed

## Setup

### 1. Install

```bash
cd /Users/todmy/github/telegram-mcp
pip install -e ".[dev]"
```

### 2. Configure

```bash
mkdir -p ~/.tg-mcp
cp .env.example ~/.tg-mcp/.env
# Edit ~/.tg-mcp/.env with your API_ID, API_HASH, PHONE
```

### 3. Authenticate (one-time)

```bash
python -m tg_mcp.auth
# Follow prompts: enter phone → enter code from Telegram → optional 2FA password
# Creates ~/.tg-mcp/session.session
```

### 4. Register MCP Server

Add to `~/.claude/settings.json` or project `.mcp.json`:

```json
{
  "mcpServers": {
    "telegram": {
      "command": "python",
      "args": ["-m", "tg_mcp"],
      "cwd": "/Users/todmy/github/telegram-mcp",
      "env": {
        "TG_MCP_DATA_DIR": "~/.tg-mcp"
      }
    }
  }
}
```

### 5. Verify

In Claude Code, try:
- "Show my Telegram channels" → triggers `tg_overview`
- "What's new in the last 24 hours?" → triggers `tg_feed`
- "What can you do with Telegram?" → triggers `tg_search_ops`

## Usage Examples

### Read channel feed
```
tg_feed channel=@llm_under_hood hours=48 limit=10
```

### Overview of all channels
```
tg_overview sort=activity folder="AI News"
```

### Find and use an operation
```
tg_search_ops query="compare"
tg_describe_op name="compare_channels"
tg_execute op="compare_channels" params={"channels": ["@ch1", "@ch2"]}
```

### React to a message
```
tg_execute op="react_to_message" params={"channel": "@llm_under_hood", "message_id": 4521, "emoji": "fire"}
```

## Running Tests

```bash
pytest tests/
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "Not authenticated" | Run `python -m tg_mcp.auth` |
| "Rate limited. Retry in Ns" | Wait N seconds, Telegram enforces this |
| "Session expired" | Re-run `python -m tg_mcp.auth` |
| "Channel not found" | Use exact @handle from `tg_overview` |
