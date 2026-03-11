"""Test MCP server using the official MCP client library.

Run this from a Databricks notebook or locally with proper auth.
"""
import asyncio
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


async def test_mcp_server():
    """Test the MCP server using official client."""

    # For local testing
    url = "http://localhost:8000/mcp"

    # For deployed app (uncomment and add token for Databricks)
    # url = "https://cdm-tools-mcp-270181971930646.6.azure.databricksapps.com/mcp"

    print(f"Connecting to MCP server: {url}")

    async with streamablehttp_client(url) as (read_stream, write_stream, _):
        async with ClientSession(read_stream, write_stream) as session:
            # Initialize the session
            await session.initialize()
            print("Session initialized!")

            # List available tools
            tools = await session.list_tools()
            print(f"\nFound {len(tools.tools)} tools:")
            for tool in tools.tools:
                print(f"  - {tool.name}: {tool.description[:60]}...")

            # Test calling a tool
            print("\n\nTesting 'lookup_cdm_fields' tool...")
            result = await session.call_tool(
                "lookup_cdm_fields",
                arguments={"cdm_name": "general_ledger_detail"}
            )
            print(f"Result: {result.content[0].text[:500]}...")


if __name__ == "__main__":
    asyncio.run(test_mcp_server())
