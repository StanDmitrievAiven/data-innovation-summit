import { Pool, PoolConfig } from "pg";

function buildPoolConfig(): PoolConfig {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is not set");
  }

  // Aiven injects PROJECT_CA_CERT (base64) when using the pg service integration.
  // pg ignores the `ssl` option when sslmode is in the URL, so strip it.
  const url = new URL(databaseUrl);
  url.searchParams.delete("sslmode");

  const caBase64 = process.env.PROJECT_CA_CERT;
  const ssl = caBase64
    ? { ca: Buffer.from(caBase64, "base64").toString() }
    : { rejectUnauthorized: false };

  return {
    connectionString: url.toString(),
    ssl,
    max: 8,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
  };
}

export const pool = new Pool(buildPoolConfig());

export async function withRetry<T>(
  fn: () => Promise<T>,
  attempts = 5,
  baseDelayMs = 500,
): Promise<T> {
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const delay = baseDelayMs * Math.pow(2, i);
      // eslint-disable-next-line no-console
      console.warn(
        `[db] attempt ${i + 1}/${attempts} failed: ${(err as Error).message}; retry in ${delay}ms`,
      );
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  throw lastErr;
}
