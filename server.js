// 아오오니 술래잡기 - 로그인 없는 멀티플레이 서버 (Node.js 18+, 의존성: ws)
// 서버가 로비/카운트다운/아오오니 선정/라운드 종료를 관리합니다.
const http = require('http'), fs = require('fs'), zlib = require('zlib'), path = require('path'), os = require('os');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000, MAX_PLAYERS = 40, TICK_MS = 50, STALE_MS = 6000;
const COUNT = +process.env.COUNT_MS || 10000;   // 입장 후 아오오니가 정해지기까지
const ROUND = +process.env.ROUND_MS || 90000;   // 아오오니가 정해진 뒤 라운드 시간
const END = +process.env.END_MS || 6000;        // 결과 화면 시간

const html = fs.readFileSync(path.join(__dirname, 'index.html'));
const NMAPS = +((/const NMAPS=(\d+)/.exec(html.toString('utf8')) || [0, 1])[1]);   // 맵 개수 (index.html과 자동 동기화)
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
  if (u === '/info') {   // 같은 와이파이용 내부 IP 안내 (QR 자동 입력에 사용)
    const lan = [];
    for (const l of Object.values(os.networkInterfaces())) for (const x of l || []) if (x.family === 'IPv4' && !x.internal) lan.push(x.address);
    lan.sort((p, q) => /^(192\.168|10\.)/.test(q) - /^(192\.168|10\.)/.test(p));
    res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ lan })); return;
  }
  res.writeHead(404); res.end('not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2048, perMessageDeflate: false });
const rooms = new Map();
let seq = 0;
const num = (v, lo, hi) => Math.min(hi, Math.max(lo, +v || 0));
const int = v => (+v | 0);
const mkRoom = () => ({ clients: new Map(), dirty: true, lastB: 0, ph: 'lobby', rd: 0, t0: 0, oni: 0, pz: 0, endAt: 0, why: '', pl: [], n0: 0, map: -1, cur: 0 });
const hostOf = room => { let h = 0; for (const c of room.clients.values()) if (c.joined && (!h || c.id < h)) h = c.id; return h; };
const endRound = (room, why) => { room.ph = 'end'; room.why = why; room.endAt = Date.now(); room.pz = 0; room.dirty = true; };

wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://x').searchParams;
  const name = (q.get('room') || 'main').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 20) || 'main';
  let room = rooms.get(name);
  if (!room) { room = mkRoom(); rooms.set(name, room); }
  if (room.clients.size >= MAX_PLAYERS) { ws.close(1013, 'full'); return; }

  const id = ++seq;
  const c = { ws, id, d: null, last: 0, seen: Date.now(), alive: true, joined: false, n: '', ch: 0 };
  room.clients.set(id, c);
  room.dirty = true;
  ws.send(JSON.stringify({ t: 'h', id, seq: id }));

  ws.on('pong', () => { c.alive = true; });
  ws.on('message', buf => {
    const now = Date.now();
    let m; try { m = JSON.parse(buf); } catch { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'p') {
      if (now - c.last < 25) return;
      c.last = now;
      const d = m.d; if (!d || typeof d !== 'object' || !c.joined) return;
      c.d = { n: c.n, x: num(d.x, 0, 700), y: num(d.y, 0, 900), c: d.c ? 1 : 0, rk: String(d.rk || '').slice(0, 12),
              dr: int(d.dr) & 3, mv: d.mv ? 1 : 0, ch: c.ch, mp: int(d.mp) & 7 };
      c.seen = now; room.dirty = true;
    } else if (m.t === 'j') {                       // 입장(닉네임/캐릭터 등록)
      c.joined = true; c.n = String(m.n || '플레이어').slice(0, 8); c.ch = int(m.ch) & 1; room.dirty = true;
    } else if (m.t === 'start') {                   // 방장이 게임 시작
      if (c.id !== hostOf(room) || room.ph !== 'lobby') return;
      room.pl = [...room.clients.values()].filter(x => x.joined).map(x => x.id);
      room.n0 = room.pl.length; room.cur = room.map < 0 ? (Math.random() * NMAPS | 0) : room.map; room.rd++; room.ph = 'count'; room.t0 = now; room.oni = 0; room.pz = 0; room.why = '';
      for (const x of room.clients.values()) x.d = null;
      room.dirty = true;
    } else if (m.t === 'map') {                     // 방장이 맵 선택 (-1 = 랜덤)
      if (c.id !== hostOf(room) || room.ph !== 'lobby') return;
      room.map = Math.max(-1, Math.min(NMAPS - 1, int(m.i))); room.dirty = true;
    } else if (m.t === 'pause') {                   // 방장이 일시정지/재개
      if (c.id !== hostOf(room) || (room.ph !== 'count' && room.ph !== 'play')) return;
      if (room.pz) { room.t0 += now - room.pz; room.pz = 0; } else room.pz = now;
      room.dirty = true;
    } else if (m.t === 'end') {                     // 아오오니가 모두 잡았다고 보고
      if (room.ph === 'play' && c.id === room.oni && !room.pz) endRound(room, 'oni');
    }
  });
  ws.on('close', () => {
    room.clients.delete(id); room.dirty = true;
    if (!room.clients.size) rooms.delete(name);
  });
});

setInterval(() => {
  const now = Date.now();
  for (const [name, room] of rooms) {
    if (!room.pz) {
      const alive = room.pl.filter(id => room.clients.has(id));
      if (room.ph === 'count') {
        if (alive.length < 1) { room.ph = 'lobby'; room.dirty = true; }
        else if (now - room.t0 >= COUNT) { room.oni = alive[Math.random() * alive.length | 0]; room.ph = 'play'; room.t0 = now; room.dirty = true; }
      } else if (room.ph === 'play') {
        if (!room.clients.has(room.oni)) endRound(room, 'gone');
        else if (room.n0 > 1 && alive.length <= 1) endRound(room, 'run');
        else if (now - room.t0 >= ROUND) endRound(room, 'run');
      } else if (room.ph === 'end' && now - room.endAt >= END) { room.ph = 'lobby'; room.dirty = true; }
    }
    if (now - room.lastB > 1000) room.dirty = true;     // 1초마다 서버 시간 동기화
    for (const c of room.clients.values()) if (c.d && now - c.seen >= STALE_MS && now - c.seen < STALE_MS + TICK_MS * 3) room.dirty = true;
    if (!room.dirty) continue;
    room.dirty = false; room.lastB = now;
    const p = [];
    if (room.ph !== 'lobby') for (const c of room.clients.values()) if (c.d && now - c.seen < STALE_MS && room.pl.includes(c.id)) p.push({ id: c.id, d: c.d });
    const lobby = [...room.clients.values()].filter(c => c.joined).map(c => ({ id: c.id, n: c.n, ch: c.ch }));
    const msg = JSON.stringify({ t: 's', p, g: { ph: room.ph, rd: room.rd, oni: room.oni, t0: room.t0, pz: room.pz, now, cnt: COUNT, rnd: ROUND, map: room.map, cur: room.cur, why: room.why, host: hostOf(room), pl: room.pl, lobby } });
    for (const c of room.clients.values()) if (c.ws.readyState === 1) c.ws.send(msg);
  }
}, TICK_MS);

setInterval(() => {
  for (const room of rooms.values()) for (const c of room.clients.values()) {
    if (!c.alive) { c.ws.terminate(); continue; }
    c.alive = false; try { c.ws.ping(); } catch {}
  }
}, 10000);

server.listen(PORT, () => console.log('서버 실행 중: http://localhost:' + PORT));
