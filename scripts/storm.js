// Fires N parallel POSTs to /ingest to simulate a real error storm.
// Usage:
//   node scripts/storm.js                   # 60 requests, default message
//   node scripts/storm.js 100               # 100 requests
//   node scripts/storm.js 60 "email failed" # custom message shape

const count   = Number(process.argv[2] ?? 60);
const message = process.argv[3] ?? 'Failed to connect to database at user id';
const url     = process.env.INGEST_URL ?? 'http://localhost:3000/ingest';

async function send(i) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      service_id: 'payments-api',
      level:      'error',
      message:    `${message} ${i}`,
    }),
  });
  return res.json();
}

const start   = Date.now();
const results = await Promise.all(
  Array.from({ length: count }, (_, i) => send(i + 1))
);
const ms = Date.now() - start;

console.log(`Fired ${count} requests in ${ms}ms  (${((count / ms) * 1000).toFixed(1)} req/s)`);
console.log(`First response: ${JSON.stringify(results[0])}`);
console.log(`Last  response: ${JSON.stringify(results.at(-1))}`);
