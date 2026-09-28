// Usage: node loadtest.js [totalRequests=3000] [concurrency=100]
const total = +process.argv[2] || 3000, conc = +process.argv[3] || 100;
let sent = 0, ok = 0, filled = 0; const lat = [];
async function worker() {
  while (sent < total) {
    const i = sent++, t = performance.now();
    const r = await (await fetch('http://localhost:3000/bid-request', { method: 'POST', body: JSON.stringify({ user: 'u' + (i % 500) }) })).json();
    lat.push(performance.now() - t); ok++; if (r.filled) filled++;
  }
}
(async () => {
  const t0 = performance.now();
  await Promise.all(Array.from({ length: conc }, worker));
  const s = (performance.now() - t0) / 1000; lat.sort((a, b) => a - b);
  console.log(`${ok} requests in ${s.toFixed(1)}s = ${(ok / s).toFixed(0)} req/s`);
  console.log(`filled: ${filled}  p50: ${lat[Math.floor(ok * .5)].toFixed(1)}ms  p99: ${lat[Math.floor(ok * .99)].toFixed(1)}ms`);
})();
