import express, { Request, Response } from "express";
import path from "path";

const PORT = parseInt(process.env.PORT ?? "3000", 10);
const API_URL = (process.env.API_URL ?? "").replace(/\/+$/, "");

if (!API_URL) {
  console.warn(
    "[boot] API_URL env var is empty. /api/* requests will fail until it is set " +
      "(via Aiven Apps app_service_name link or manual env var).",
  );
}

const app = express();

app.get("/healthz", (_req, res) => res.json({ ok: true, api: API_URL || null }));

app.use("/api", async (req: Request, res: Response) => {
  if (!API_URL) {
    res.status(503).json({ error: "API_URL not configured" });
    return;
  }
  const target = `${API_URL}${req.url}`;
  const headers: Record<string, string> = { accept: "application/json" };
  try {
    const upstream = await fetch(target, { method: req.method, headers });
    res.status(upstream.status);
    upstream.headers.forEach((v, k) => {
      if (k.toLowerCase() === "content-encoding") return;
      res.setHeader(k, v);
    });
    const buf = Buffer.from(await upstream.arrayBuffer());
    res.send(buf);
  } catch (err) {
    console.error("[proxy]", target, (err as Error).message);
    res.status(502).json({ error: "upstream fetch failed", target });
  }
});

const distPath = path.resolve(__dirname, "..", "dist");
app.use(express.static(distPath));

// SPA fallback: any non-API, non-static path returns index.html
app.get("*", (_req, res) => {
  res.sendFile(path.join(distPath, "index.html"));
});

app.listen(PORT, "0.0.0.0", () =>
  console.log(
    `[boot] dashboard-web listening on 0.0.0.0:${PORT} -> API=${API_URL || "(not set)"}`,
  ),
);
