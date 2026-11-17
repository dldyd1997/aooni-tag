// 아오오니 술래잡기 - 로그인 없는 멀티플레이 서버 (Node.js 18+, 의존성: ws)
const http = require('http'), fs = require('fs'), zlib = require('zlib'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000, MAX_PLAYERS = 40, TICK_MS = 50, STALE_MS = 6000;

const html = fs.readFileSync(path.join(__dirname, 'index.html'));
const gz = zlib.gzipSync(html, { level: 9 });

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/healthz') { res.end('ok'); return; }
  if (u === '/' || u === '/index.html') {
    const h = { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' };
    if (/\bgzip\b/.test(req.headers['accept-encoding'] || '')) { h['Content-Encoding'] = 'gzip'; res.writeHead(200, h); res.end(gz); }
    else { res.writeHead(200, h); res.end(html); }
    return;
  }
  res.writeHead(404); res.end('not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2048, perMessageDeflate: false });
const rooms = new Map();           // 방 이름 -> { clients: Map(id -> client), dirty }
let seq = 0;                       // 접속 순서 번호 (가장 작은 번호가 아오오니)
const num = (v, lo, hi) => Math.min(hi, Math.max(lo, +v || 0));
const int = v => (+v | 0);

wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://x').searchParams;
  const name = (q.get('room') || 'main').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 20) || 'main';
  let room = rooms.get(name);
  if (!room) { room = { clients: new Map(), dirty: false }; rooms.set(name, room); }
  if (room.clients.size >= MAX_PLAYERS) { ws.close(1013, 'full'); return; }

  const id = ++seq;
  const c = { ws, id, d: null, last: 0, seen: Date.now(), alive: true };
  room.clients.set(id, c);
  ws.send(JSON.stringify({ t: 'h', id, seq: id }));

  ws.on('pong', () => { c.alive = true; });
  ws.on('message', buf => {
    const now = Date.now();
    if (now - c.last < 25) return;            // 초당 40회 이상은 무시
    c.last = now;
    let m; try { m = JSON.parse(buf); } catch { return; }
    if (m.t !== 'p' || !m.d || typeof m.d !== 'object') return;
    const d = m.d;
    c.d = { n: String(d.n || '').slice(0, 8), x: num(d.x, 0, 700), y: num(d.y, 0, 500), t: id,
            c: d.c ? 1 : 0, rd: int(d.rd), rs: num(d.rs, 0, 9e15), rk: String(d.rk || '').slice(0, 40),
            col: String(d.col || '').slice(0, 9), dr: int(d.dr) & 3, mv: d.mv ? 1 : 0, pz: num(d.pz, 0, 9e15), ch: int(d.ch) & 1 };
    c.seen = now; room.dirty = true;
  });
  ws.on('close', () => {
    room.clients.delete(id); room.dirty = true;
    if (!room.clients.size) rooms.delete(name);
  });
});

// 상태 브로드캐스트 (초당 20회, 변화가 있을 때만)
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    if (!room.dirty) continue;
    room.dirty = false;
    const p = [];
    for (const c of room.clients.values()) if (c.d && now - c.seen < STALE_MS) p.push({ id: c.id, d: c.d });
    const msg = JSON.stringify({ t: 's', p });
    for (const c of room.clients.values()) if (c.ws.readyState === 1) c.ws.send(msg);
  }
}, TICK_MS);

// 오래 멈춘 탭 정리 + 끊긴 연결 감지
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    for (const c of room.clients.values()) {
      if (c.d && now - c.seen >= STALE_MS) room.dirty = true;   // 유령 플레이어 제거 반영
      if (!c.alive) { c.ws.terminate(); continue; }
      c.alive = false; try { c.ws.ping(); } catch {}
    }
  }
}, 10000);

server.listen(PORT, () => console.log('서버 실행 중: http://localhost:' + PORT));
