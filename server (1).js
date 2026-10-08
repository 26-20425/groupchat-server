const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, "messages.json");
const MAX_HISTORY = 500;
const MAX_TEXT = 1000;
const MAX_NAME = 16;

// ---- 메시지 저장 (파일) ----
let messages = [];
try {
  messages = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
} catch (e) {
  messages = [];
}
let saveTimer = null;
function saveSoon() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    fs.writeFile(DATA_FILE, JSON.stringify(messages), () => {});
  }, 1000);
}

// ---- HTTP: 정적 파일(index.html) ----
const INDEX = path.join(__dirname, "public", "index.html");
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200);
    return res.end("ok");
  }
  fs.readFile(INDEX, (err, buf) => {
    if (err) {
      res.writeHead(500);
      return res.end("index.html not found");
    }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(buf);
  });
});

// ---- WebSocket ----
const wss = new WebSocketServer({ server, maxPayload: 8 * 1024 });

function broadcast(obj) {
  const data = JSON.stringify(obj);
  for (const c of wss.clients) if (c.readyState === 1) c.send(data);
}
function sendOnline() {
  broadcast({ type: "online", count: wss.clients.size });
}

wss.on("connection", (ws) => {
  ws.lastSent = 0;
  ws.send(JSON.stringify({ type: "history", messages }));
  sendOnline();

  ws.on("message", (raw) => {
    let m;
    try {
      m = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }
    if (!m || m.type !== "msg") return;

    const text = String(m.text || "").trim().slice(0, MAX_TEXT);
    const name = String(m.name || "익명").trim().slice(0, MAX_NAME) || "익명";
    const uid = String(m.uid || "").slice(0, 32);
    if (!text) return;

    // 간단한 도배 방지: 0.5초에 1개
    const now = Date.now();
    if (now - ws.lastSent < 500) return;
    ws.lastSent = now;

    const msg = { uid, name, text, ts: now };
    messages.push(msg);
    if (messages.length > MAX_HISTORY) messages = messages.slice(-MAX_HISTORY);
    saveSoon();
    broadcast({ type: "msg", message: msg });
  });

  ws.on("close", sendOnline);
  ws.on("error", () => {});
});

// 끊긴 연결 정리 (30초마다 ping)
setInterval(() => {
  for (const c of wss.clients) {
    if (c.isAlive === false) {
      c.terminate();
      continue;
    }
    c.isAlive = false;
    c.ping();
  }
}, 30000);
wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));
});

server.listen(PORT, () => console.log("채팅 서버 실행 중: http://localhost:" + PORT));
