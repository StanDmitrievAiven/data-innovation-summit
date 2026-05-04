import { createClient, ClickHouseClient } from "@clickhouse/client";

function required(name: string): string {
  const v = process.env[name];
  if (!v || v === "REPLACE_ME_VIA_AIVEN_CONSOLE") {
    throw new Error(
      `${name} is not set. For Aiven deploys, set it on the application service via Aiven Console -> dashboard-api -> Variables.`,
    );
  }
  return v;
}

export const ch: ClickHouseClient = createClient({
  url: `https://${required("CLICKHOUSE_HOST")}:${process.env.CLICKHOUSE_PORT ?? "14209"}`,
  username: required("CLICKHOUSE_USER"),
  password: required("CLICKHOUSE_PASSWORD"),
  database: process.env.CLICKHOUSE_DATABASE ?? "default",
  request_timeout: 15_000,
  application: "dashboard-api",
});

export async function query<T>(
  sql: string,
  query_params: Record<string, unknown> = {},
): Promise<T[]> {
  const result = await ch.query({
    query: sql,
    query_params,
    format: "JSONEachRow",
  });
  return await result.json<T>();
}
