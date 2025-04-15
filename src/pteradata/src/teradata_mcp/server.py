import argparse
import asyncio
import logging
import os
import signal
import re
from typing import Optional
from enum import Enum
from typing import Any
from typing import List
from typing import Union
import teradatasql
from urllib.parse import urlparse
from pydantic import Field
from .tdsql import obfuscate_password
from .tdsql import TDConn


import mcp.types as types
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP("teradata-mcp")
ResponseType = List[types.TextContent | types.ImageContent | types.EmbeddedResource]


_tdconn = TDConn()

def format_text_response(text: Any) -> ResponseType:
    """Format a text response."""
    return [types.TextContent(type="text", text=str(text))]


def format_error_response(error: str) -> ResponseType:
    """Format an error response."""
    return format_text_response(f"Error: {error}")

@mcp.tool(description=f"Execute any SQL query")
async def execute_sql(
    sql: str = Field(description="SQL to run", default="all"),
) -> ResponseType:
    """Executes a SQL query against the database."""
    global _tdconn
    try:
        cur = _tdconn.cursor()
        rows = cur.execute(sql)  # type: ignore
        if rows is None:
            return format_text_response("No results")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return format_error_response(str(e))

@mcp.tool(description="List all databases in the Teradata system")
async def list_db() -> ResponseType:
    """List all databases in the Teradata."""
    try:
        global _tdconn
        cur = _tdconn.cursor()
        rows = cur.execute("select DataBaseName, DECODE(DBKind, 'U', 'User', 'D','DataBase') as DBType , CommentString from dbc.DatabasesV dv where OwnerName <> 'PDCRADM'")
        return format_text_response(list([row for row in rows.fetchall()]))
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        return format_error_response(str(e))


async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Teradata MCP Server")
    parser.add_argument("database_url", help="Database connection URL", nargs="?")
    

    args = parser.parse_args()

    
    mcp.add_tool(execute_sql, description="Execute any SQL query")
    
    logger.info(f"Starting Teradata MCP Server")

    # Get database URL from environment variable or command line
    database_url = os.environ.get("DATABASE_URI", args.database_url)

    if not database_url:
        raise ValueError(
            "Error: No database URL provided. Please specify via 'DATABASE_URI' environment variable or command-line argument.",
        )

    global _tdconn
    # Initialize database connection pool
    try:
        _tdconn = TDConn(database_url)
        logger.info("Successfully connected to database and initialized connection")
    except Exception as e:
        logger.warning(
            f"Could not connect to database: {obfuscate_password(str(e))}",
        )
        logger.warning(
            "The MCP server will start but database operations will fail until a valid connection is established.",
        )

    # Set up proper shutdown handling
    try:
        loop = asyncio.get_running_loop()
        signals = (signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s)))
    except NotImplementedError:
        # Windows doesn't support signals properly
        logger.warning("Signal handling not supported on Windows")
        pass

    await mcp.run_stdio_async()

async def shutdown(sig=None):
    """Clean shutdown of the server."""
    global shutdown_in_progress

    if shutdown_in_progress:
        logger.warning("Forcing immediate exit")

        os._exit(1)  # Use immediate process termination instead of sys.exit

    shutdown_in_progress = True

    if sig:
        logger.info(f"Received exit signal {sig.name}")

    os._exit(128 + sig if sig is not None else 0)

if __name__ == "__main__":
    asyncio.run(main())
