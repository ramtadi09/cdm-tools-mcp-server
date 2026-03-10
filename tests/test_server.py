"""Integration tests for the CDM Tools MCP Server."""

import os
import shlex
import signal
import socket
import subprocess
import time
from contextlib import closing

import pytest
import requests


def _find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_server(url: str, timeout: int = 15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=1)
            if 200 <= resp.status_code < 400:
                return resp
        except Exception:
            pass
        time.sleep(0.2)
    raise TimeoutError(f"Server at {url} did not respond in {timeout}s")


@pytest.fixture(scope="session")
def server_url():
    port = _find_free_port()
    url = f"http://127.0.0.1:{port}"
    cmd = shlex.split(f"uv run cdm-mcp-server --port {port}")
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        preexec_fn=os.setsid,
    )
    try:
        _wait_for_server(url)
    except Exception:
        proc.terminate()
        raise
    yield url
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=10)
    except Exception:
        os.killpg(proc.pid, signal.SIGKILL)


def test_health(server_url):
    resp = requests.get(server_url)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"


def test_list_tools(server_url):
    """Test that MCP tools are discoverable via the DatabricksMCPClient."""
    try:
        from databricks_mcp import DatabricksMCPClient
    except ImportError:
        pytest.skip("databricks-mcp not installed (dev dependency)")

    client = DatabricksMCPClient(server_url=f"{server_url}/mcp")
    tools = client.list_tools()
    tool_names = {t.name for t in tools}

    expected = {
        "analyze_files", "lookup_erp_columns",
        "lookup_cdm_fields", "find_past_mappings",
        "preview_transform", "lookup_pipeline_notebook",
        "setup_databricks_job", "generate_transform_notebook",
        "validate_data",
    }
    assert expected.issubset(tool_names), f"Missing tools: {expected - tool_names}"
