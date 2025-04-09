#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import * as teradatasql from "teradatasql";

const server = new Server(
  {
    name: "example-servers/teradata",
    version: "0.1.0",
  },
  {
    capabilities: {
      resources: {},
      tools: {},
    },
  },
);

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("Please provide a database URL as a command-line argument");
  process.exit(1);
}

const databaseUrl = args[0];

const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "teradata:";

const sHost: string = resourceBaseUrl.hostname;
const sUser: string = resourceBaseUrl.username;
const sPassword: string = resourceBaseUrl.password;

type Rows = any[];
type Row = any[] | null;

const SCHEMA_PATH = "schema";
const con: teradatasql.TeradataConnection = teradatasql.connect({ host: sHost, user: sUser, password: sPassword });

server.setRequestHandler(ListResourcesRequestSchema, async () => {
    const cur: teradatasql.TeradataCursor = con.cursor();
    try {
      //if (sDB.length ===0) {
      await cur.execute("select TableName from dbc.TablesV tv, dbc.UsersV uv where UPPER(uv.UserName) = UPPER('"+sUser+"') and UPPER(tv.DataBaseName) = UPPER(uv.DefaultDatabase) and tv.TableKind in ('T','V');");
      //}
      //else {
      //cur.execute("select tv.TableName from dbc.TablesV tv where UPPER(tv.DatabaseName) = UPPER('wtest') and tv.TableKind in ('T','V')");
      //}
      const rows: Rows = cur.fetchall();

      return { resources: rows.map((row) => ({
        uri: new URL(`${row.TableName}/${SCHEMA_PATH}`, resourceBaseUrl).href,
        mimeType: "application/json",
        name: `"${row.TableName}" database schema`,
      })), 
    }; 
  } finally {
    cur.close();
  }
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const resourceUrl = new URL(request.params.uri);

  const pathComponents = resourceUrl.pathname.split("/");
  const schema = pathComponents.pop();
  const tableName = pathComponents.pop();

  if (schema !== SCHEMA_PATH) {
    throw new Error("Invalid resource URI");
  }
  //const con: teradatasql.TeradataConnection = teradatasql.connect({ host: sHost, user: sUser, password: sPassword });
  const cur: teradatasql.TeradataCursor = con.cursor();
  try {
    cur.execute(`sel ColumnName, CASE ColumnType 
        WHEN '++' THEN 'TD_ANYTYPE'
          WHEN 'A1' THEN 'UDT'
          WHEN 'AT' THEN 'TIME'
          WHEN 'BF' THEN 'BYTE'
          WHEN 'BO' THEN 'BLOB'
          WHEN 'BV' THEN 'VARBYTE'
          WHEN 'CF' THEN 'CHAR'
          WHEN 'CO' THEN 'CLOB'
          WHEN 'CV' THEN 'VARCHAR'
          WHEN 'D' THEN  'DECIMAL'
          WHEN 'DA' THEN 'DATE'
          WHEN 'DH' THEN 'INTERVAL DAY TO HOUR'
          WHEN 'DM' THEN 'INTERVAL DAY TO MINUTE'
          WHEN 'DS' THEN 'INTERVAL DAY TO SECOND'
          WHEN 'DY' THEN 'INTERVAL DAY'
          WHEN 'F' THEN  'FLOAT'
          WHEN 'HM' THEN 'INTERVAL HOUR TO MINUTE'
          WHEN 'HR' THEN 'INTERVAL HOUR'
          WHEN 'HS' THEN 'INTERVAL HOUR TO SECOND'
          WHEN 'I1' THEN 'BYTEINT'
          WHEN 'I2' THEN 'SMALLINT'
          WHEN 'I8' THEN 'BIGINT'
          WHEN 'I' THEN  'INTEGER'
          WHEN 'MI' THEN 'INTERVAL MINUTE'
          WHEN 'MO' THEN 'INTERVAL MONTH'
          WHEN 'MS' THEN 'INTERVAL MINUTE TO SECOND'
          WHEN 'N' THEN 'NUMBER'
          WHEN 'PD' THEN 'PERIOD(DATE)'
          WHEN 'PM' THEN 'PERIOD(TIMESTAMP WITH TIME ZONE)'
          WHEN 'PS' THEN 'PERIOD(TIMESTAMP)'
          WHEN 'PT' THEN 'PERIOD(TIME)'
          WHEN 'PZ' THEN 'PERIOD(TIME WITH TIME ZONE)'
          WHEN 'SC' THEN 'INTERVAL SECOND'
          WHEN 'SZ' THEN 'TIMESTAMP WITH TIME ZONE'
          WHEN 'TS' THEN 'TIMESTAMP'
          WHEN 'TZ' THEN 'TIME WITH TIME ZONE'
          WHEN 'UT' THEN 'UDT'
          WHEN 'YM' THEN 'INTERVAL YEAR TO MONTH'
          WHEN 'YR' THEN 'INTERVAL YEAR'
          WHEN 'AN' THEN 'UDT'
          WHEN 'XM' THEN 'XML'
          WHEN 'JN' THEN 'JSON'
          WHEN 'DT' THEN 'DATASET'
          WHEN '??' THEN 'STGEOMETRY''ANY_TYPE'
          END as CType
      from DBC.ColumnsVX where tableName = $1`);
      const rows: Rows = await cur.fetchall();
    return {
      contents: [
        {
          uri: request.params.uri,
          mimeType: "application/json",
          text: JSON.stringify(rows, null, 2),
        },
      ],
    };
  } finally {
    cur.close();
  }
});

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "query",
        description: "Run a read-only SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request: {
  params: {
    name: string;
    arguments?: {
      sql?: string;
    };
  };
}) => {
  if (request.params.name === "query") {
    const sql: string = request.params.arguments?.sql as string;
    //const con: teradatasql.TeradataConnection = await teradatasql.connect({ host: sHost, user: sUser, password: sPassword });
    const cur: teradatasql.TeradataCursor = await con.cursor();
    try {
      await cur.execute(sql);
      const rows: Rows = cur.fetchall();
      return {
        content: [{ type: "text", text: JSON.stringify(rows, null, 2) }],
        isError: false,
      };
    } catch (error) {
      throw error;
    } finally {
      cur.close();
    }
  }
  throw new Error(`Unknown tool: ${request.params.name}`);
});

// Start server
async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Secure MCP Filesystem Server running on stdio");
}

runServer().catch((error) => {
  console.error("Fatal error running server:", error);
  process.exit(1);
});