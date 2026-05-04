# dashboard-web

The visual layer of the Data Innovation Summit demo: a single-page React app showing live KPIs, top products, regional breakdown, and a live order feed — all driven by `dashboard-api`, which queries `clickhouse-2a6274d2`.

## Architecture

```
Browser
   |
   v
dashboard-web (this app)
  ├── serves dist/  (Vite-built React SPA)
  └── /api/*  -->  proxy to API_URL (= dashboard-api public URL)
```

Aiven Apps injects `API_URL` automatically when this app is deployed with `app_service_name: "dashboard-api"`.

## Tech

- React 18 + Vite + TypeScript
- Recharts (charts)
- Express (static + tiny JSON proxy)

## Local development

```bash
npm install
npm run build:web    # produces dist/
npm run build:server # produces dist-server/
API_URL=https://dashboard-api-host npm start
# in another terminal: vite dev with proxy to localhost:3000
```

For dev with hot reload of the frontend:

```bash
npm run dev          # vite dev server on :5173, proxies /api to :3000
# and in another shell, run dashboard-api locally on :3000
```

## Why proxy instead of direct calls

Vite envs are baked at build time, so we'd need to know the API URL before the Docker build. Instead, the React app just calls same-origin `/api/...` and the Express server forwards to whatever `API_URL` is at runtime — meaning we can swap out the API target with one env var change in Aiven Console without rebuilding.
