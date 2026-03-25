"""
Entry point for MCP clients that run `python server.py` from this directory.

Prefer: `uv run mysql-mcp-server-ms` (see README.md).
"""

from mysql_mcp_server import main

if __name__ == "__main__":
    main()
