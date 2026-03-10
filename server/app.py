"""FastAPI + MCP server setup.

Creates the FastMCP server, registers CDM tools, and combines
MCP routes with a health-check endpoint into a single ASGI app.
"""

from fastapi import FastAPI, Request
from fastmcp import FastMCP

from .tools import register_tools
from .utils import header_store

# Create the MCP server
mcp_server = FastMCP(name="cdm-tools-mcp-server")

# Register all 9 CDM tools
register_tools(mcp_server)

# Convert to streamable-HTTP ASGI app (serves at /mcp)
mcp_app = mcp_server.http_app()

# Separate FastAPI for non-MCP endpoints
api = FastAPI(title="CDM Tools MCP Server", version="0.1.0", lifespan=mcp_app.lifespan)


@api.get("/")
async def health():
    return {"status": "healthy", "server": "cdm-tools-mcp-server"}


# Combined app: MCP routes + API routes
combined_app = FastAPI(
    title="CDM Tools MCP Server",
    routes=[*mcp_app.routes, *api.routes],
    lifespan=mcp_app.lifespan,
)


@combined_app.middleware("http")
async def capture_headers(request: Request, call_next):
    """Capture request headers for OBO authentication in tools."""
    header_store.set(dict(request.headers))
    return await call_next(request)
