const http = require("http");
const fs = require("fs");
const path = require("path");

const port = Number(process.env.PORT) || 8080;
const root = __dirname;
const files = {
  "/": "public/index.html",
  "/index.html": "public/index.html",
  "/main.js": "public/main.js",
  "/three.module.js": "node_modules/three/build/three.module.js",
};
const types = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
};

const server = http.createServer((req, res) => {
  const rel = files[(req.url || "/").split("?")[0]];
  if (!rel) {
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
    return;
  }
  fs.readFile(path.join(root, rel), (err, data) => {
    if (err) {
      res.writeHead(500, { "content-type": "text/plain" });
      res.end("missing file");
      return;
    }
    res.writeHead(200, {
      "content-type": types[path.extname(rel)] || "application/octet-stream",
      "cache-control": "no-cache",
    });
    res.end(data);
  });
});

server.listen(port, "0.0.0.0");
