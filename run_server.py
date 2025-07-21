#!/usr/bin/env python3
"""
Run the MCP server
"""

from mcp_server import mcp

if __name__ == "__main__":
    print("Starting MCP server...")
    print("Server will be available via MCP protocol")
    print("Press Ctrl+C to stop the server")
    
    # Run the server
    mcp.run() 