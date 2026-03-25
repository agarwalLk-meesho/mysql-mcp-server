from .server import mcp


def main():
    """CLI entry point for the MCP MySQL server."""
    mcp.run()


__all__ = ["main", "mcp"]
