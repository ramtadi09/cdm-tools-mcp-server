"""FastAPI + MCP server setup.

Creates the FastMCP server, registers CDM tools, and combines
MCP routes with a health-check endpoint into a single ASGI app.
"""

import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastmcp import FastMCP

from .tools import register_tools
from .utils import header_store

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create the MCP server
logger.info("=" * 70)
logger.info("MCP SERVER: Initializing CDM Tools MCP Server")
logger.info("=" * 70)

mcp_server = FastMCP(name="cdm-tools-mcp-server")

# Register all 9 CDM tools
logger.info("MCP SERVER: Registering CDM tools...")
register_tools(mcp_server)
logger.info("MCP SERVER: Tools registered successfully")

# Convert to streamable-HTTP ASGI app (serves at /mcp)
mcp_app = mcp_server.http_app()

# Separate FastAPI for non-MCP endpoints
api = FastAPI(title="CDM Tools MCP Server", version="0.1.0", lifespan=mcp_app.lifespan)


@api.get("/")
async def health():
    logger.info("HEALTH: Health check endpoint called")
    return {"status": "healthy", "server": "cdm-tools-mcp-server"}


# Combined app: MCP routes + API routes
combined_app = FastAPI(
    title="CDM Tools MCP Server",
    routes=[*mcp_app.routes, *api.routes],
    lifespan=mcp_app.lifespan,
)

# Add CORS middleware to handle preflight OPTIONS requests
combined_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Databricks Apps handles auth, allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods including OPTIONS
    allow_headers=["*"],  # Allow all headers
)


@combined_app.middleware("http")
async def capture_headers(request: Request, call_next):
    """Capture request headers for OBO authentication in tools."""

    # Log incoming request
    logger.info("=" * 70)
    logger.info(f"REQUEST: {request.method} {request.url.path}")
    logger.info(f"REQUEST: Client: {request.client.host if request.client else 'unknown'}")
    logger.info("=" * 70)

    # Capture headers for auth
    headers_dict = dict(request.headers)
    header_store.set(headers_dict)

    # Log auth-related headers (masked)
    auth_headers_found = []
    if "x-forwarded-access-token" in headers_dict:
        auth_headers_found.append("x-forwarded-access-token (AI Playground User)")
    if "authorization" in headers_dict:
        auth_headers_found.append("Authorization (M2M or direct call)")

    if auth_headers_found:
        logger.info(f"REQUEST: Auth headers detected: {', '.join(auth_headers_found)}")
    else:
        logger.warning("REQUEST: No auth headers found in request!")

    # Process request
    response = await call_next(request)

    # Log response
    logger.info(f"RESPONSE: Status {response.status_code} for {request.method} {request.url.path}")
    logger.info("-" * 70)

    return response


logger.info("MCP SERVER: Server initialization complete")
logger.info(f"MCP SERVER: Endpoints available:")
logger.info(f"MCP SERVER:   - GET  /       (health check)")
logger.info(f"MCP SERVER:   - POST /mcp    (MCP protocol)")
