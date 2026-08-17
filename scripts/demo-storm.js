// Fires 5 distinct realistic failure storms in parallel.
// Each storm produces a unique fingerprint → a unique Gemini diagnosis.
// Usage:  node scripts/demo-storm.js  [perStorm=60]

const perStorm = Number(process.argv[2] ?? 60);
const only     = process.argv[3];  // optional substring filter, e.g. "orders"
const url      = process.env.INGEST_URL ?? 'http://localhost:3000/ingest';

// Each scenario simulates a real ops incident. The `id` in the message
// varies per request so the messages look natural, but they normalize
// to the same fingerprint after the variable-token step.
const scenarios = [
  {
    label:      'DB pool exhaustion',
    service_id: 'payments-api',
    message:    (i) => `Failed to acquire DB connection from pool after 5000ms (pool size: 20, waiting: ${i})`,
  },
  {
    label:      'Downstream service timeout',
    service_id: 'orders-api',
    message:    (i) => `HTTP GET http://inventory-service:8080/check/${i} timed out after 3000ms`,
  },
  {
    label:      'OAuth token refresh failure',
    service_id: 'notifications-worker',
    message:    (i) => `Token refresh failed for tenant ${i}: HTTP 401 invalid_grant (client_id: sender-svc)`,
  },
  {
    label:      'Payment gateway rate limit',
    service_id: 'checkout-api',
    message:    (i) => `Stripe API returned 429 rate_limit_exceeded on POST /v1/charges (request ${i})`,
  },
  {
    label:      'Redis cache miss cascade',
    service_id: 'analytics-worker',
    message:    (i) => `Redis GET session:${i} returned nil after 5000ms, falling back to Postgres query`,
  },
];

async function send(service_id, message) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ service_id, level: 'error', message }),
  });
  return res.json();
}

async function fireStorm({ label, service_id, message }) {
  const start = Date.now();
  await Promise.all(
    Array.from({ length: perStorm }, (_, i) => send(service_id, message(i + 1)))
  );
  const ms = Date.now() - start;
  console.log(`  [${service_id.padEnd(22)}] ${perStorm} logs in ${ms}ms  — ${label}`);
}

const active = only
  ? scenarios.filter((s) => s.service_id.includes(only) || s.label.toLowerCase().includes(only.toLowerCase()))
  : scenarios;

if (only && active.length === 0) {
  console.error(`No scenario matches "${only}". Available: ${scenarios.map(s => s.service_id).join(', ')}`);
  process.exit(1);
}

console.log(`Firing ${active.length} parallel storm(s) × ${perStorm} logs each = ${active.length * perStorm} total requests\n`);

const start = Date.now();
await Promise.all(active.map(fireStorm));
const totalMs = Date.now() - start;

console.log(`\nAll storms complete in ${totalMs}ms.`);
console.log(`Expect ${active.length} escalation(s) → ${active.length} Gemini call(s) → ${active.length} incident(s).`);
