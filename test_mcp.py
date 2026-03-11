"""Quick test script for the MCP server."""
import requests
import json

# For deployed Databricks App (requires OAuth - run from Databricks notebook)
DEPLOYED_URL = "https://cdm-tools-mcp-270181971930646.6.azure.databricksapps.com"

# For local testing
LOCAL_URL = "http://localhost:8000"


def test_health(base_url):
    """Test the health endpoint."""
    try:
        resp = requests.get(f"{base_url}/", timeout=10)
        print(f"Health check: {resp.status_code}")
        print(f"Response: {resp.json()}")
        return resp.status_code == 200
    except Exception as e:
        print(f"Health check failed: {e}")
        return False


def test_mcp_tools(base_url):
    """Test MCP tools/list endpoint."""
    # MCP uses JSON-RPC over HTTP with Streamable HTTP transport
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    }
    try:
        resp = requests.post(
            f"{base_url}/mcp",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream"
            },
            timeout=10
        )
        print(f"MCP tools/list: {resp.status_code}")
        data = resp.json()
        if "result" in data and "tools" in data["result"]:
            tools = data["result"]["tools"]
            print(f"Found {len(tools)} tools:")
            for tool in tools:
                print(f"  - {tool['name']}: {tool.get('description', '')[:60]}...")
        else:
            print(f"Response: {json.dumps(data, indent=2)}")
        return True
    except Exception as e:
        print(f"MCP test failed: {e}")
        return False


def test_tool_call(base_url, tool_name, arguments):
    """Test calling a specific MCP tool."""
    payload = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments
        }
    }
    try:
        resp = requests.post(
            f"{base_url}/mcp",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream"
            },
            timeout=30
        )
        print(f"\nTool '{tool_name}' call: {resp.status_code}")
        data = resp.json()
        print(f"Response: {json.dumps(data, indent=2)[:500]}...")
        return True
    except Exception as e:
        print(f"Tool call failed: {e}")
        return False


if __name__ == "__main__":
    import sys

    url = LOCAL_URL
    if len(sys.argv) > 1 and sys.argv[1] == "--deployed":
        url = DEPLOYED_URL
        print("Note: Deployed URL requires OAuth token. Run from Databricks notebook.")

    print(f"Testing MCP server at: {url}\n")
    print("=" * 50)

    # Test health
    print("\n1. Testing health endpoint...")
    test_health(url)

    # Test MCP tools list
    print("\n2. Testing MCP tools/list...")
    test_mcp_tools(url)

    # Test a simple tool call (lookup_cdm_fields)
    print("\n3. Testing tool call (lookup_cdm_fields)...")
    test_tool_call(url, "lookup_cdm_fields", {"cdm_name": "general_ledger_detail"})
