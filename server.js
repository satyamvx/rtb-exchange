// Mini Real-Time Bidding Ad Exchange (zero dependencies, Node 18+)
// Run: node server.js  ->  http://localhost:3000
const http = require('http');
const PORT = process.env.PORT || 3000;
const fs = require('fs');
const path = require('path');
const { EventEmitter } = require('events');

// ---------- Advertisers (mock bidders run in-process with simulated latency) ----------
const advertisers = [
  { id: 'nike',   name: 'Nike',   budget: 500,  base: 1.2, maxFreq: 3 },
  { id: 'amazon', name: 'Amazon', budget: 800,  base: 1.5, maxFreq: 5 },
  { id: 'zomato', name: 'Zomato', budget: 300,  base: 0.9, maxFreq: 2 },
  { id: 'uber',   name: 'Uber',   budget: 400,  base: 1.1, maxFreq: 4 },
].map(a => ({ ...a, spent: 0 }));

// ---------- Redis-like store (atomic in Node's single thread; swap for Redis INCRBYFLOAT/INCR) ----------
const store = {
  spend: new Map(),      // advertiserId -> spent
  freq: new Map(),       // `${user}:${adv}` -> impressions in window (TTL 1h)
  seenClicks: new Set(), // dedup: impressionId already clicked
  pending: new Map(),    // impressionId -> {adv, price, user}
  reserve(advId, amount, budget) { // atomic check-and-increment: never overspend
    const cur = this.spend.get(advId) || 0;
    if (cur + amount > budget) return false;
    this.spend.set(advId, cur + amount);
    return true;
  },
  refund(advId, amount) { this.spend.set(advId, (this.spend.get(advId) || 0) - amount); },
  freqCount(user, advId) { const e = this.freq.get(`${user}:${advId}`); return e && e.exp > Date.now() ? e.n : 0; },
  freqIncr(user, advId) {
    const k = `${user}:${advId}`, e = this.freq.get(k);
    if (e && e.exp > Date.now()) e.n++; else this.freq.set(k, { n: 1, exp: Date.now() + 3600e3 });
  },
};

// ---------- Event pipeline (Kafka stand-in: topic + consumer, append-only log on disk) ----------
const topic = new EventEmitter();
const LOG = path.join(__dirname, 'events.ndjson');
const analytics = {}; // advertiserId -> {bids, wins, impressions, clicks, spend}
advertisers.forEach(a => analytics[a.id] = { bids: 0, wins: 0, impressions: 0, clicks: 0, spend: 0 });
const logStream = fs.createWriteStream(LOG, { flags: 'a' });
topic.on('event', e => { // consumer: persist + aggregate (ClickHouse stand-in)
  logStream.write(JSON.stringify(e) + '\n');
  const s = analytics[e.adv]; if (!s) return;
  if (e.type === 'impression') { s.impressions++; s.spend += e.price; }
  if (e.type === 'click') s.clicks++;
});
const publish = e => topic.emit('event', { ts: Date.now(), ...e });

// ---------- Bidding ----------
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function askBid(adv, req) {
  await sleep(Math.random() * 30);                      // simulated network latency
  if (Math.random() < 0.1) return null;                 // 10% no-bid
  const boost = req.category === 'sports' && adv.id === 'nike' ? 1.5 : 1;
  return { adv: adv.id, price: +(adv.base * boost * (0.6 + Math.random() * 0.8)).toFixed(4) };
}
const withTimeout = (p, ms) => Promise.race([p, sleep(ms).then(() => null)]);

const metrics = { requests: 0, noFill: 0, lat: [] };

async function runAuction(req) {
  const t0 = process.hrtime.bigint();
  metrics.requests++;
  // 1) filter eligible advertisers: budget left + frequency cap
  const eligible = advertisers.filter(a =>
    (store.spend.get(a.id) || 0) < a.budget && store.freqCount(req.user, a.id) < a.maxFreq);
  // 2) collect bids in parallel with a hard 60ms timeout
  const bids = (await Promise.all(eligible.map(a => withTimeout(askBid(a, req), 60)))).filter(Boolean)
    .sort((x, y) => y.price - x.price);
  let result = { filled: false };
  // 3) second-price auction: winner pays runner-up price (+0.01), or own bid if alone
  for (let i = 0; i < bids.length && !result.filled; i++) {
    const win = bids[i];
    const price = +Math.min(win.price, (bids[i + 1]?.price ?? win.price - 0.01) + 0.01).toFixed(4);
    const adv = advertisers.find(a => a.id === win.adv);
    if (!store.reserve(adv.id, price, adv.budget)) continue; // atomic budget guard; fall to next bidder
    store.freqIncr(req.user, adv.id);
    const impressionId = 'imp_' + Math.random().toString(36).slice(2, 10);
    store.pending.set(impressionId, { adv: adv.id, price, user: req.user });
    analytics[adv.id].wins++;
    publish({ type: 'impression', impressionId, adv: adv.id, price, user: req.user });
    result = { filled: true, impressionId, winner: adv.name, price, bids: bids.length };
  }
  bids.forEach(b => analytics[b.adv].bids++);
  if (!result.filled) metrics.noFill++;
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  metrics.lat.push(ms); if (metrics.lat.length > 5000) metrics.lat.shift();
  return { ...result, latencyMs: +ms.toFixed(2) };
}

function click(impressionId) {
  const imp = store.pending.get(impressionId);
  if (!imp) return { ok: false, reason: 'unknown impression' };
  if (store.seenClicks.has(impressionId)) return { ok: false, reason: 'duplicate click (fraud)' };
  store.seenClicks.add(impressionId);
  publish({ type: 'click', impressionId, adv: imp.adv, user: imp.user });
  return { ok: true };
}

const pct = (arr, p) => { if (!arr.length) return 0; const s = [...arr].sort((a, b) => a - b); return +s[Math.floor(s.length * p)].toFixed(2); };

// ---------- HTTP ----------
const json = (res, code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
const readBody = req => new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d || '{}')); } catch { r({}); } }); });

http.createServer(async (req, res) => {
  if (req.method === 'POST' && req.url === '/bid-request') {
    const b = await readBody(req);
    return json(res, 200, await runAuction({ user: b.user || 'anon', category: b.category || 'general' }));
  }
  if (req.method === 'POST' && req.url === '/click') return json(res, 200, click((await readBody(req)).impressionId));
  if (req.method === 'POST' && req.url === '/reset') { // demo helper: reset budgets & stats
    store.spend.clear(); store.freq.clear(); store.seenClicks.clear(); store.pending.clear();
    Object.keys(analytics).forEach(k => analytics[k] = { bids: 0, wins: 0, impressions: 0, clicks: 0, spend: 0 });
    metrics.requests = 0; metrics.noFill = 0; metrics.lat = [];
    return json(res, 200, { ok: true });
  }
  if (req.url === '/health') return json(res, 200, { ok: true });
  if (req.url === '/stats') {
    return json(res, 200, {
      requests: metrics.requests, noFill: metrics.noFill,
      p50: pct(metrics.lat, 0.5), p99: pct(metrics.lat, 0.99),
      advertisers: advertisers.map(a => {
        const s = analytics[a.id];
        return { id: a.id, name: a.name, budget: a.budget, ...s, spend: +s.spend.toFixed(2),
          ctr: s.impressions ? +(100 * s.clicks / s.impressions).toFixed(1) : 0 };
      }),
    });
  }
  if (req.url === '/' || req.url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    return res.end(fs.readFileSync(path.join(__dirname, 'dashboard.html')));
  }
  json(res, 404, { error: 'not found' });
}).listen(PORT, '0.0.0.0', () => console.log('RTB exchange on port ' + PORT));
