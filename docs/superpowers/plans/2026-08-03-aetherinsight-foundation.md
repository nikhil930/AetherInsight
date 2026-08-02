# AetherInsight Foundation (Weeks 1–2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the vertical slice from HTTP ingestion through Kafka, fingerprint-based windowing in Redis, and a stubbed analyzer, ending with an incident row in Postgres.

**Architecture:** One repo, four Node process entry points over a shared core. Logs POST to a Fastify API which produces to Redpanda. A filter worker fingerprints each message, records it in a Redis sliding window, and on threshold breach claims and forwards an analysis request. An AI worker consumes that, calls an analyzer behind an interface (stubbed in this plan, Groq in the next), and writes a structured incident to Postgres.

**Tech Stack:** Node.js 20+ (ESM), Fastify 4, KafkaJS 2, ioredis 5, pg 8, zod 3, pino 9, vitest 2, Docker Compose (Redpanda, Redis, Postgres).

## Global Constraints

- Node.js 20 or later. All source is ESM (`"type": "module"` in `package.json`).
- Project root is `D:\Projects\AetherInsight`. All paths below are relative to it.
- Kafka broker address from the host is `localhost:19092`; from inside Compose it is `redpanda:9092`.
- Topic names exactly: `raw-logs`, `ai-analysis-requests`, `diagnosed-incidents`, `dead-letter`.
- `raw-logs` and `ai-analysis-requests` have 3 partitions, keyed by `service_id`. `diagnosed-incidents` and `dead-letter` have 1.
- Envelope version is `1` for all messages in this plan.
- Auto topic creation is **disabled**; topics are created by an explicit script.
- No ORM. Postgres access is `pg` with hand-written SQL.
- Default thresholds: window 10 seconds, alert threshold 50 occurrences, claim TTL 900 seconds, 20 context samples.
- Every task ends by appending its Q&A block to `D:\Interview_material_NR\Claude Project QnA.txt`. **Append only — never rewrite or reorder the file.**

## File Structure

| File | Responsibility |
|---|---|
| `docker-compose.yml` | Redpanda, Redpanda Console, Redis, Postgres |
| `schema.sql` | Postgres tables, auto-applied on first container boot |
| `scripts/create-topics.js` | Creates the four topics with correct partition counts |
| `src/shared/config.js` | Environment parsing and validation |
| `src/shared/logger.js` | Pino instance |
| `src/shared/events.js` | Versioned envelope, per-type zod payload schemas, encode/decode |
| `src/shared/kafka.js` | Kafka client factory, producer helper, consumer runner, shutdown hook |
| `src/shared/redis.js` | ioredis client, sliding-window Lua command, claim and context helpers |
| `src/shared/db.js` | Postgres pool and incident insert |
| `src/shared/fingerprint.js` | Message normalization and hashing |
| `src/api/server.js` | Fastify ingestion API |
| `src/workers/filter.js` | Fingerprint, window, claim, forward |
| `src/workers/ai.js` | Consume analysis requests, analyze, persist |
| `src/workers/analyzers/schema.js` | Zod schema every analyzer must satisfy |
| `src/workers/analyzers/fake.js` | Stub analyzer used in this plan |

---

### Task 1: Infrastructure and project scaffold

**Files:**
- Create: `package.json`, `.gitignore`, `.env.example`, `docker-compose.yml`, `schema.sql`, `src/shared/logger.js`

**Interfaces:**
- Consumes: nothing
- Produces: `logger` (default export from `src/shared/logger.js`), a running Compose stack on `localhost:19092` (Kafka), `localhost:6379` (Redis), `localhost:5432` (Postgres), `localhost:8080` (Redpanda Console)

- [ ] **Step 1: Initialize the repo**

```bash
cd /d/Projects/AetherInsight
git init
npm init -y
npm pkg set type=module
npm install fastify@^4.28.0 kafkajs@^2.2.4 ioredis@^5.4.1 pg@^8.12.0 zod@^3.23.8 pino@^9.4.0
npm install -D vitest@^2.1.0 pino-pretty@^11.2.2
```

- [ ] **Step 2: Create `.gitignore`**

```
node_modules/
.env
coverage/
*.log
```

- [ ] **Step 3: Create `.env.example`**

```
KAFKA_BROKERS=localhost:19092
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgres://aether:aether@localhost:5432/aetherinsight
API_PORT=3000
LOG_LEVEL=debug
WINDOW_SECONDS=10
ALERT_THRESHOLD=50
CLAIM_TTL_SECONDS=900
CONTEXT_SAMPLES=20
```

- [ ] **Step 4: Create `docker-compose.yml`**

```yaml
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.7
    container_name: aether-redpanda
    command:
      - redpanda
      - start
      - --mode dev-container
      - --smp 1
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
    ports:
      - "19092:19092"
      - "9644:9644"
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:.*true'"]
      interval: 5s
      retries: 20

  console:
    image: docker.redpanda.com/redpandadata/console:v2.7.2
    container_name: aether-console
    depends_on:
      - redpanda
    ports:
      - "8080:8080"
    environment:
      KAFKA_BROKERS: redpanda:9092

  redis:
    image: redis:7-alpine
    container_name: aether-redis
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      retries: 10

  postgres:
    image: postgres:16-alpine
    container_name: aether-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: aether
      POSTGRES_PASSWORD: aether
      POSTGRES_DB: aetherinsight
    volumes:
      - ./schema.sql:/docker-entrypoint-initdb.d/01-schema.sql:ro
      - aether-pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U aether -d aetherinsight"]
      interval: 5s
      retries: 10

volumes:
  aether-pgdata:
```

- [ ] **Step 5: Create `schema.sql`**

```sql
CREATE TABLE IF NOT EXISTS incidents (
  id               uuid PRIMARY KEY,
  service_id       text NOT NULL,
  fingerprint      text NOT NULL,
  title            text NOT NULL,
  summary          text NOT NULL,
  probable_cause   text,
  suggested_fix    text,
  confidence       real,
  severity         text NOT NULL,
  occurrence_count int  NOT NULL,
  window_seconds   int  NOT NULL,
  sample_logs      jsonb NOT NULL,
  llm_model        text,
  llm_tokens       int,
  llm_latency_ms   int,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS incidents_service_created_idx
  ON incidents (service_id, created_at DESC);
CREATE INDEX IF NOT EXISTS incidents_fingerprint_created_idx
  ON incidents (fingerprint, created_at DESC);

CREATE TABLE IF NOT EXISTS dead_letters (
  id           uuid PRIMARY KEY,
  source_topic text NOT NULL,
  payload      jsonb NOT NULL,
  error        text NOT NULL,
  attempts     int  NOT NULL DEFAULT 0,
  created_at   timestamptz NOT NULL DEFAULT now()
);
```

- [ ] **Step 6: Create `src/shared/logger.js`**

```js
import pino from 'pino';

const isDev = process.env.NODE_ENV !== 'production';

export const logger = pino({
  level: process.env.LOG_LEVEL ?? 'info',
  transport: isDev ? { target: 'pino-pretty', options: { colorize: true } } : undefined,
});

export default logger;
```

- [ ] **Step 7: Bring the stack up and verify**

```bash
docker compose up -d
docker compose ps
```

Expected: four containers, `redpanda`, `redis` and `postgres` reporting `healthy`.

- [ ] **Step 8: Verify Postgres applied the schema**

```bash
docker compose exec postgres psql -U aether -d aetherinsight -c "\dt"
```

Expected: `incidents` and `dead_letters` listed.

- [ ] **Step 9: Verify Redis responds**

```bash
docker compose exec redis redis-cli ping
```

Expected: `PONG`

- [ ] **Step 10: Commit**

```bash
git add .
git commit -m "chore: scaffold project and docker compose infrastructure"
```

- [ ] **Step 11: Append Q&A block 1 to `D:\Interview_material_NR\Claude Project QnA.txt`** (content supplied in the "Q&A Blocks" section at the end of this plan)

---

### Task 2: Config loading

**Files:**
- Create: `src/shared/config.js`, `test/config.test.js`
- Modify: `package.json` (add `test` script)

**Interfaces:**
- Consumes: nothing
- Produces: `loadConfig(env: object) → Config`, and `config: Config` (parsed from `process.env` at import). `Config` fields: `KAFKA_BROKERS: string`, `REDIS_URL: string`, `DATABASE_URL: string`, `API_PORT: number`, `LOG_LEVEL: string`, `WINDOW_SECONDS: number`, `ALERT_THRESHOLD: number`, `CLAIM_TTL_SECONDS: number`, `CONTEXT_SAMPLES: number`

- [ ] **Step 1: Add the test script to `package.json`**

```bash
npm pkg set scripts.test="vitest run"
npm pkg set scripts.test:watch="vitest"
```

- [ ] **Step 2: Write the failing test — `test/config.test.js`**

```js
import { describe, it, expect } from 'vitest';
import { loadConfig } from '../src/shared/config.js';

describe('loadConfig', () => {
  it('applies defaults when env is empty', () => {
    const cfg = loadConfig({});
    expect(cfg.KAFKA_BROKERS).toBe('localhost:19092');
    expect(cfg.ALERT_THRESHOLD).toBe(50);
    expect(cfg.WINDOW_SECONDS).toBe(10);
  });

  it('coerces numeric strings to numbers', () => {
    const cfg = loadConfig({ ALERT_THRESHOLD: '5', API_PORT: '4000' });
    expect(cfg.ALERT_THRESHOLD).toBe(5);
    expect(cfg.API_PORT).toBe(4000);
  });

  it('throws on a non-numeric threshold', () => {
    expect(() => loadConfig({ ALERT_THRESHOLD: 'banana' })).toThrow();
  });
});
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `npx vitest run test/config.test.js`
Expected: FAIL — cannot resolve `../src/shared/config.js`

- [ ] **Step 4: Write `src/shared/config.js`**

```js
import { z } from 'zod';

const schema = z.object({
  NODE_ENV:           z.string().default('development'),
  LOG_LEVEL:          z.string().default('info'),
  KAFKA_BROKERS:      z.string().default('localhost:19092'),
  REDIS_URL:          z.string().default('redis://localhost:6379'),
  DATABASE_URL:       z.string().default('postgres://aether:aether@localhost:5432/aetherinsight'),
  API_PORT:           z.coerce.number().int().positive().default(3000),
  WINDOW_SECONDS:     z.coerce.number().int().positive().default(10),
  ALERT_THRESHOLD:    z.coerce.number().int().positive().default(50),
  CLAIM_TTL_SECONDS:  z.coerce.number().int().positive().default(900),
  CONTEXT_SAMPLES:    z.coerce.number().int().positive().default(20),
});

export function loadConfig(env) {
  const result = schema.safeParse(env);
  if (!result.success) {
    throw new Error(`Invalid configuration:\n${JSON.stringify(result.error.flatten().fieldErrors, null, 2)}`);
  }
  return result.data;
}

export const config = loadConfig(process.env);
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `npx vitest run test/config.test.js`
Expected: PASS — 3 tests

- [ ] **Step 6: Commit**

```bash
git add src/shared/config.js test/config.test.js package.json
git commit -m "feat: add validated config loading"
```

- [ ] **Step 7: Append Q&A block 2 to the Q&A file**

---

### Task 3: Event envelope and payload schemas

**Files:**
- Create: `src/shared/events.js`, `test/events.test.js`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `SCHEMA_VERSION: number` (= 1)
  - `encode(type: string, payload: object) → string` — returns a JSON string ready for Kafka
  - `decode(buffer: Buffer|string) → { v, type, id, ts, payload }` — throws on unknown type, wrong version, or invalid payload
  - `rawLogSchema`, `analysisRequestSchema` — zod schemas

- [ ] **Step 1: Write the failing test — `test/events.test.js`**

```js
import { describe, it, expect } from 'vitest';
import { encode, decode, SCHEMA_VERSION } from '../src/shared/events.js';

const validLog = { service_id: 'payments', level: 'error', message: 'db timeout' };

describe('encode', () => {
  it('wraps a payload in a versioned envelope', () => {
    const parsed = JSON.parse(encode('log.raw', validLog));
    expect(parsed.v).toBe(SCHEMA_VERSION);
    expect(parsed.type).toBe('log.raw');
    expect(parsed.id).toMatch(/^[0-9a-f-]{36}$/);
    expect(parsed.payload.service_id).toBe('payments');
  });

  it('rejects a payload that fails its schema', () => {
    expect(() => encode('log.raw', { service_id: 'x', level: 'nonsense', message: 'y' })).toThrow();
  });

  it('rejects an unknown event type', () => {
    expect(() => encode('log.imaginary', validLog)).toThrow(/Unknown event type/);
  });
});

describe('decode', () => {
  it('round-trips an encoded message', () => {
    const out = decode(Buffer.from(encode('log.raw', validLog)));
    expect(out.payload.message).toBe('db timeout');
  });

  it('rejects an unsupported envelope version', () => {
    const bad = JSON.stringify({
      v: 99, type: 'log.raw', id: '00000000-0000-4000-8000-000000000000',
      ts: new Date().toISOString(), payload: validLog,
    });
    expect(() => decode(bad)).toThrow(/Unsupported envelope version/);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/events.test.js`
Expected: FAIL — cannot resolve `../src/shared/events.js`

- [ ] **Step 3: Write `src/shared/events.js`**

```js
import { z } from 'zod';
import { randomUUID } from 'node:crypto';

export const SCHEMA_VERSION = 1;

export const rawLogSchema = z.object({
  service_id: z.string().min(1).max(64),
  level:      z.enum(['debug', 'info', 'warn', 'error', 'fatal']),
  message:    z.string().min(1).max(8192),
  stack:      z.string().max(16384).optional(),
  trace_id:   z.string().max(128).optional(),
  meta:       z.record(z.unknown()).optional(),
});

export const analysisRequestSchema = z.object({
  service_id:       z.string().min(1),
  fingerprint:      z.string().length(40),
  occurrence_count: z.number().int().positive(),
  window_seconds:   z.number().int().positive(),
  sample_logs:      z.array(z.string()).min(1),
});

const PAYLOADS = {
  'log.raw':            rawLogSchema,
  'analysis.requested': analysisRequestSchema,
};

function schemaFor(type) {
  const schema = PAYLOADS[type];
  if (!schema) throw new Error(`Unknown event type: ${type}`);
  return schema;
}

export function encode(type, payload) {
  return JSON.stringify({
    v: SCHEMA_VERSION,
    type,
    id: randomUUID(),
    ts: new Date().toISOString(),
    payload: schemaFor(type).parse(payload),
  });
}

const envelopeSchema = z.object({
  v:    z.number().int(),
  type: z.string(),
  id:   z.string().uuid(),
  ts:   z.string(),
  payload: z.unknown(),
});

export function decode(input) {
  const outer = envelopeSchema.parse(JSON.parse(input.toString()));
  if (outer.v !== SCHEMA_VERSION) {
    throw new Error(`Unsupported envelope version ${outer.v} (expected ${SCHEMA_VERSION})`);
  }
  return { ...outer, payload: schemaFor(outer.type).parse(outer.payload) };
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/events.test.js`
Expected: PASS — 5 tests

- [ ] **Step 5: Commit**

```bash
git add src/shared/events.js test/events.test.js
git commit -m "feat: add versioned event envelope with per-type schemas"
```

- [ ] **Step 6: Append Q&A block 3 to the Q&A file**

---

### Task 4: Kafka client, producer, consumer runner, topic creation

**Files:**
- Create: `src/shared/kafka.js`, `scripts/create-topics.js`
- Modify: `package.json` (add `topics` script)

**Interfaces:**
- Consumes: `config` from Task 2, `logger` from Task 1
- Produces:
  - `createKafka(clientId: string) → Kafka`
  - `createProducer(clientId: string) → Promise<Producer>` (connected)
  - `runConsumer({ clientId, groupId, topics, handler }) → Promise<Consumer>` where `handler({ topic, partition, message })` is async
  - `onShutdown(fn: () => Promise<void>) → void` — registers SIGINT/SIGTERM handling with a 10s force-exit timer
  - Topic constants: `TOPICS.RAW_LOGS`, `TOPICS.ANALYSIS_REQUESTS`, `TOPICS.DIAGNOSED`, `TOPICS.DEAD_LETTER`

- [ ] **Step 1: Write `src/shared/kafka.js`**

```js
import { Kafka, logLevel } from 'kafkajs';
import { config } from './config.js';
import { logger } from './logger.js';

export const TOPICS = {
  RAW_LOGS:          'raw-logs',
  ANALYSIS_REQUESTS: 'ai-analysis-requests',
  DIAGNOSED:         'diagnosed-incidents',
  DEAD_LETTER:       'dead-letter',
};

export function createKafka(clientId) {
  return new Kafka({
    clientId,
    brokers: config.KAFKA_BROKERS.split(',').map((b) => b.trim()),
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 8 },
  });
}

export async function createProducer(clientId) {
  const producer = createKafka(clientId).producer({ allowAutoTopicCreation: false });
  await producer.connect();
  logger.info({ clientId }, 'producer connected');
  return producer;
}

export async function runConsumer({ clientId, groupId, topics, handler }) {
  const consumer = createKafka(clientId).consumer({ groupId });
  await consumer.connect();
  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }
  logger.info({ clientId, groupId, topics }, 'consumer subscribed');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await handler({ topic, partition, message });
    },
  });

  return consumer;
}

export function onShutdown(fn) {
  let shuttingDown = false;
  const run = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutdown started');
    const force = setTimeout(() => {
      logger.error('shutdown timed out after 10s, forcing exit');
      process.exit(1);
    }, 10_000);
    force.unref();
    try {
      await fn();
      logger.info('shutdown complete');
      process.exit(0);
    } catch (err) {
      logger.error({ err }, 'shutdown failed');
      process.exit(1);
    }
  };
  process.on('SIGTERM', () => run('SIGTERM'));
  process.on('SIGINT',  () => run('SIGINT'));
}
```

- [ ] **Step 2: Write `scripts/create-topics.js`**

```js
import { createKafka, TOPICS } from '../src/shared/kafka.js';
import { logger } from '../src/shared/logger.js';

const admin = createKafka('topic-admin').admin();
await admin.connect();

const desired = [
  { topic: TOPICS.RAW_LOGS,          numPartitions: 3, replicationFactor: 1 },
  { topic: TOPICS.ANALYSIS_REQUESTS, numPartitions: 3, replicationFactor: 1 },
  { topic: TOPICS.DIAGNOSED,         numPartitions: 1, replicationFactor: 1 },
  { topic: TOPICS.DEAD_LETTER,       numPartitions: 1, replicationFactor: 1 },
];

const existing = await admin.listTopics();
const missing = desired.filter((t) => !existing.includes(t.topic));

if (missing.length === 0) {
  logger.info('all topics already exist');
} else {
  await admin.createTopics({ topics: missing, waitForLeaders: true });
  logger.info({ created: missing.map((t) => t.topic) }, 'topics created');
}

await admin.disconnect();
```

- [ ] **Step 3: Add the script to `package.json`**

```bash
npm pkg set scripts.topics="node scripts/create-topics.js"
```

- [ ] **Step 4: Run it and verify**

Run: `npm run topics`
Expected: log line listing all four topics created.

Run it a second time.
Expected: `all topics already exist` — the script is idempotent.

- [ ] **Step 5: Verify partition counts in the browser**

Open `http://localhost:8080` (Redpanda Console) → Topics.
Expected: `raw-logs` and `ai-analysis-requests` show 3 partitions; the other two show 1.

- [ ] **Step 6: Commit**

```bash
git add src/shared/kafka.js scripts/create-topics.js package.json
git commit -m "feat: add kafka client helpers and topic creation script"
```

- [ ] **Step 7: Append Q&A block 4 to the Q&A file**

---

### Task 5: Ingestion API

**Files:**
- Create: `src/api/server.js`, `test/api.test.js`
- Modify: `package.json` (add `start:api` script)

**Interfaces:**
- Consumes: `encode`, `rawLogSchema` from Task 3; `createProducer`, `TOPICS`, `onShutdown` from Task 4; `config` from Task 2
- Produces: `buildApp(producer) → FastifyInstance` — accepts any object with a `send({ topic, messages })` method, so tests inject a fake

- [ ] **Step 1: Write the failing test — `test/api.test.js`**

```js
import { describe, it, expect, beforeEach } from 'vitest';
import { buildApp } from '../src/api/server.js';

function fakeProducer() {
  const sent = [];
  return { sent, send: async (payload) => { sent.push(payload); } };
}

let producer, app;
beforeEach(async () => {
  producer = fakeProducer();
  app = await buildApp(producer);
});

const log = { service_id: 'payments', level: 'error', message: 'db timeout' };

describe('POST /v1/logs', () => {
  it('accepts a single log and returns 202', async () => {
    const res = await app.inject({ method: 'POST', url: '/v1/logs', payload: log });
    expect(res.statusCode).toBe(202);
    expect(res.json()).toEqual({ accepted: 1 });
  });

  it('produces to raw-logs keyed by service_id', async () => {
    await app.inject({ method: 'POST', url: '/v1/logs', payload: log });
    expect(producer.sent).toHaveLength(1);
    expect(producer.sent[0].topic).toBe('raw-logs');
    expect(producer.sent[0].messages[0].key).toBe('payments');
  });

  it('accepts a batch array', async () => {
    const res = await app.inject({ method: 'POST', url: '/v1/logs', payload: [log, log, log] });
    expect(res.json()).toEqual({ accepted: 3 });
    expect(producer.sent[0].messages).toHaveLength(3);
  });

  it('rejects an invalid payload with 400 and does not produce', async () => {
    const res = await app.inject({
      method: 'POST', url: '/v1/logs',
      payload: { service_id: 'payments', level: 'nonsense', message: 'x' },
    });
    expect(res.statusCode).toBe(400);
    expect(producer.sent).toHaveLength(0);
  });

  it('serves /health', async () => {
    const res = await app.inject({ method: 'GET', url: '/health' });
    expect(res.statusCode).toBe(200);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/api.test.js`
Expected: FAIL — cannot resolve `../src/api/server.js`

- [ ] **Step 3: Write `src/api/server.js`**

```js
import Fastify from 'fastify';
import { z } from 'zod';
import { config } from '../shared/config.js';
import { logger } from '../shared/logger.js';
import { encode, rawLogSchema } from '../shared/events.js';
import { createProducer, TOPICS, onShutdown } from '../shared/kafka.js';

const bodySchema = z.union([rawLogSchema, z.array(rawLogSchema).min(1).max(500)]);

export async function buildApp(producer) {
  const app = Fastify({ logger: false, bodyLimit: 5 * 1024 * 1024 });

  app.get('/health', async () => ({ ok: true }));

  app.post('/v1/logs', async (request, reply) => {
    const parsed = bodySchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({
        error: 'invalid_payload',
        details: parsed.error.flatten(),
      });
    }

    const logs = Array.isArray(parsed.data) ? parsed.data : [parsed.data];

    await producer.send({
      topic: TOPICS.RAW_LOGS,
      messages: logs.map((entry) => ({
        key: entry.service_id,
        value: encode('log.raw', entry),
      })),
    });

    return reply.code(202).send({ accepted: logs.length });
  });

  return app;
}

// Entry point — skipped when the module is imported by tests.
if (process.argv[1]?.endsWith('server.js')) {
  const producer = await createProducer('ingest-api');
  const app = await buildApp(producer);
  await app.listen({ port: config.API_PORT, host: '0.0.0.0' });
  logger.info({ port: config.API_PORT }, 'ingest api listening');

  onShutdown(async () => {
    await app.close();
    await producer.disconnect();
  });
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/api.test.js`
Expected: PASS — 5 tests

- [ ] **Step 5: Add the start script and run the API for real**

```bash
npm pkg set scripts.start:api="node src/api/server.js"
npm run start:api
```

In a second terminal:

```bash
curl -X POST http://localhost:3000/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{"service_id":"payments","level":"error","message":"db timeout"}'
```

Expected: `{"accepted":1}`. Then open `http://localhost:8080` → Topics → `raw-logs` → Messages, and confirm the envelope is visible.

- [ ] **Step 6: Commit**

```bash
git add src/api/server.js test/api.test.js package.json
git commit -m "feat: add ingestion api producing to raw-logs"
```

- [ ] **Step 7: Append Q&A block 5 to the Q&A file**

---

### Task 6: Filter worker skeleton — Week 1 milestone

**Files:**
- Create: `src/workers/filter.js`
- Modify: `package.json` (add `start:filter` script)

**Interfaces:**
- Consumes: `runConsumer`, `TOPICS`, `onShutdown` from Task 4; `decode` from Task 3
- Produces: a running consumer in group `filter-workers`. Replaced in Task 10.

- [ ] **Step 1: Write `src/workers/filter.js`**

```js
import { runConsumer, TOPICS, onShutdown } from '../shared/kafka.js';
import { decode } from '../shared/events.js';
import { logger } from '../shared/logger.js';

const consumer = await runConsumer({
  clientId: 'filter-worker',
  groupId: 'filter-workers',
  topics: [TOPICS.RAW_LOGS],
  handler: async ({ partition, message }) => {
    try {
      const event = decode(message.value);
      logger.info({
        partition,
        offset: message.offset,
        service: event.payload.service_id,
        message: event.payload.message,
      }, 'log received');
    } catch (err) {
      // Poison-pill isolation: a throw here would be retried forever and
      // stall this partition for every service on it. Task 10 routes these
      // to the dead-letter topic; for now, log and drop.
      logger.error({ err, partition, offset: message.offset }, 'undecodable message dropped');
    }
  },
});

onShutdown(async () => { await consumer.disconnect(); });
```

- [ ] **Step 2: Add the start script**

```bash
npm pkg set scripts.start:filter="node src/workers/filter.js"
```

- [ ] **Step 3: Verify the Week 1 milestone end to end**

Terminal 1: `npm run start:api`
Terminal 2: `npm run start:filter`
Terminal 3:

```bash
curl -X POST http://localhost:3000/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{"service_id":"payments","level":"error","message":"db timeout"}'
```

Expected: terminal 2 prints a `log received` line with the service, partition and offset.

- [ ] **Step 4: Verify partition keying**

Post logs for three different services:

```bash
for s in payments auth search; do
  curl -s -X POST http://localhost:3000/v1/logs -H 'Content-Type: application/json' \
    -d "{\"service_id\":\"$s\",\"level\":\"error\",\"message\":\"boom\"}"
done
```

Expected: the same `service_id` always lands on the same partition across repeated runs.

- [ ] **Step 5: Verify graceful shutdown**

Press `Ctrl+C` in terminal 2.
Expected: `shutdown started` then `shutdown complete`, exiting within a second — not a hang and not an abrupt kill.

- [ ] **Step 6: Commit**

```bash
git add src/workers/filter.js package.json
git commit -m "feat: add filter worker consuming raw-logs"
```

- [ ] **Step 7: Append Q&A block 6 to the Q&A file**

---

### Task 7: Fingerprinting

**Files:**
- Create: `src/shared/fingerprint.js`, `test/fingerprint.test.js`

**Interfaces:**
- Consumes: nothing
- Produces: `normalize(message: string) → string`, `fingerprint(message: string) → string` (40-char sha1 hex)

- [ ] **Step 1: Write the failing test — `test/fingerprint.test.js`**

```js
import { describe, it, expect } from 'vitest';
import { normalize, fingerprint } from '../src/shared/fingerprint.js';

const cases = [
  ['Connection to db-7 timed out after 3021ms for user 4471',
   'Connection to db-<*> timed out after <*>ms for user <*>'],
  ['Request 550e8400-e29b-41d4-a716-446655440000 failed',
   'Request <*> failed'],
  ['Cannot reach 192.168.1.14 on port 5432',
   'Cannot reach <*> on port <*>'],
  ['Invalid token "abc123xyz" supplied',
   'Invalid token <*> supplied'],
  ['Segfault at 0xdeadbeef',
   'Segfault at <*>'],
];

describe('normalize', () => {
  it.each(cases)('normalizes %s', (input, expected) => {
    expect(normalize(input)).toBe(expected);
  });

  it('collapses repeated whitespace', () => {
    expect(normalize('too    many\tspaces')).toBe('too many spaces');
  });
});

describe('fingerprint', () => {
  it('returns a 40-char sha1 hex string', () => {
    expect(fingerprint('anything')).toMatch(/^[0-9a-f]{40}$/);
  });

  it('gives the same fingerprint to structurally identical messages', () => {
    const a = fingerprint('Timeout after 30ms for user 1');
    const b = fingerprint('Timeout after 9999ms for user 88888');
    expect(a).toBe(b);
  });

  it('gives different fingerprints to structurally different messages', () => {
    expect(fingerprint('Timeout after 30ms')).not.toBe(fingerprint('Refused after 30ms'));
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/fingerprint.test.js`
Expected: FAIL — cannot resolve `../src/shared/fingerprint.js`

- [ ] **Step 3: Write `src/shared/fingerprint.js`**

```js
import { createHash } from 'node:crypto';

// Order matters. Broader patterns must run before narrower ones, or a
// narrow rule will chew up part of a token the broad rule needed intact.
// UUIDs contain digits, so the number rule must not run before the UUID rule.
const RULES = [
  [/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi, '<*>'], // uuid
  [/\b\d{1,3}(?:\.\d{1,3}){3}\b/g, '<*>'],   // ipv4
  [/\b0x[0-9a-f]+\b/gi, '<*>'],              // hex literal
  [/\b[0-9a-f]{16,}\b/gi, '<*>'],            // long hex id / hash
  [/"[^"]*"|'[^']*'/g, '<*>'],               // quoted literal
  [/\b\d+(?:\.\d+)?\b/g, '<*>'],             // any remaining number
  [/\s+/g, ' '],                             // whitespace collapse
];

export function normalize(message) {
  let out = message;
  for (const [pattern, replacement] of RULES) {
    out = out.replace(pattern, replacement);
  }
  return out.trim();
}

export function fingerprint(message) {
  return createHash('sha1').update(normalize(message)).digest('hex');
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/fingerprint.test.js`
Expected: PASS — 9 tests

- [ ] **Step 5: Commit**

```bash
git add src/shared/fingerprint.js test/fingerprint.test.js
git commit -m "feat: add log fingerprinting via template normalization"
```

- [ ] **Step 6: Append Q&A block 7 to the Q&A file**

---

### Task 8: Redis sliding window, claiming, and context buffer

**Files:**
- Create: `src/shared/redis.js`, `test/redis.test.js`

**Interfaces:**
- Consumes: `config` from Task 2
- Produces:
  - `createRedis() → Redis` — an ioredis client with the `slidingWindow` command defined
  - `recordOccurrence(redis, { serviceId, fingerprint, windowSeconds, now? }) → Promise<number>` — returns the count inside the window
  - `claimAnalysis(redis, fingerprint, ttlSeconds) → Promise<boolean>`
  - `releaseClaim(redis, fingerprint) → Promise<void>`
  - `pushContext(redis, fingerprint, message, max, ttlSeconds) → Promise<void>`
  - `readContext(redis, fingerprint) → Promise<string[]>`

**Note:** these tests run against the real Redis from Compose. Ensure `docker compose up -d` is running.

- [ ] **Step 1: Write the failing test — `test/redis.test.js`**

```js
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import {
  createRedis, recordOccurrence, claimAnalysis,
  releaseClaim, pushContext, readContext,
} from '../src/shared/redis.js';

let redis;
beforeAll(() => { redis = createRedis(); });
afterAll(async () => { await redis.quit(); });
beforeEach(async () => { await redis.flushdb(); });

describe('recordOccurrence', () => {
  it('counts occurrences inside the window', async () => {
    const args = { serviceId: 'payments', fingerprint: 'abc', windowSeconds: 10 };
    expect(await recordOccurrence(redis, args)).toBe(1);
    expect(await recordOccurrence(redis, args)).toBe(2);
    expect(await recordOccurrence(redis, args)).toBe(3);
  });

  it('evicts occurrences older than the window', async () => {
    const base = Date.now();
    const args = { serviceId: 'payments', fingerprint: 'abc', windowSeconds: 10 };
    await recordOccurrence(redis, { ...args, now: base - 60_000 });
    await recordOccurrence(redis, { ...args, now: base - 30_000 });
    expect(await recordOccurrence(redis, { ...args, now: base })).toBe(1);
  });

  it('keeps separate windows per service and fingerprint', async () => {
    await recordOccurrence(redis, { serviceId: 'payments', fingerprint: 'abc', windowSeconds: 10 });
    const other = await recordOccurrence(redis, { serviceId: 'auth', fingerprint: 'abc', windowSeconds: 10 });
    expect(other).toBe(1);
  });
});

describe('claimAnalysis', () => {
  it('grants the claim exactly once', async () => {
    expect(await claimAnalysis(redis, 'abc', 60)).toBe(true);
    expect(await claimAnalysis(redis, 'abc', 60)).toBe(false);
  });

  it('grants again after the claim is released', async () => {
    await claimAnalysis(redis, 'abc', 60);
    await releaseClaim(redis, 'abc');
    expect(await claimAnalysis(redis, 'abc', 60)).toBe(true);
  });
});

describe('context buffer', () => {
  it('keeps at most max samples, newest first', async () => {
    for (let i = 1; i <= 5; i++) await pushContext(redis, 'abc', `msg ${i}`, 3, 60);
    expect(await readContext(redis, 'abc')).toEqual(['msg 5', 'msg 4', 'msg 3']);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/redis.test.js`
Expected: FAIL — cannot resolve `../src/shared/redis.js`

- [ ] **Step 3: Write `src/shared/redis.js`**

```js
import Redis from 'ioredis';
import { randomUUID } from 'node:crypto';
import { config } from './config.js';

// One atomic script per occurrence. Running these four commands as separate
// round trips would let two workers both read 49, both add one, and both
// observe 50 — producing two analyses for one incident. Executing them
// server-side makes the read-modify-check indivisible, and costs one round
// trip instead of four.
const SLIDING_WINDOW_LUA = `
  local key      = KEYS[1]
  local now      = tonumber(ARGV[1])
  local windowMs = tonumber(ARGV[2])
  local member   = ARGV[3]
  local ttl      = tonumber(ARGV[4])

  redis.call('ZREMRANGEBYSCORE', key, 0, now - windowMs)
  redis.call('ZADD', key, now, member)
  redis.call('EXPIRE', key, ttl)
  return redis.call('ZCARD', key)
`;

export function createRedis() {
  const redis = new Redis(config.REDIS_URL, { maxRetriesPerRequest: 3 });
  redis.defineCommand('slidingWindow', { numberOfKeys: 1, lua: SLIDING_WINDOW_LUA });
  return redis;
}

export async function recordOccurrence(redis, { serviceId, fingerprint, windowSeconds, now = Date.now() }) {
  const key = `win:${serviceId}:${fingerprint}`;
  return redis.slidingWindow(key, now, windowSeconds * 1000, randomUUID(), windowSeconds * 2);
}

export async function claimAnalysis(redis, fingerprint, ttlSeconds) {
  const result = await redis.set(`claim:${fingerprint}`, 'inflight', 'NX', 'EX', ttlSeconds);
  return result === 'OK';
}

export async function releaseClaim(redis, fingerprint) {
  await redis.del(`claim:${fingerprint}`);
}

export async function pushContext(redis, fingerprint, message, max, ttlSeconds) {
  const key = `ctx:${fingerprint}`;
  await redis.multi()
    .lpush(key, message)
    .ltrim(key, 0, max - 1)
    .expire(key, ttlSeconds)
    .exec();
}

export async function readContext(redis, fingerprint) {
  return redis.lrange(`ctx:${fingerprint}`, 0, -1);
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/redis.test.js`
Expected: PASS — 6 tests

- [ ] **Step 5: Inspect the keys by hand to build intuition**

```bash
docker compose exec redis redis-cli
> KEYS *
> ZRANGE win:payments:abc 0 -1 WITHSCORES
> TTL claim:abc
> LRANGE ctx:abc 0 -1
> exit
```

- [ ] **Step 6: Commit**

```bash
git add src/shared/redis.js test/redis.test.js
git commit -m "feat: add redis sliding window, claiming and context buffer"
```

- [ ] **Step 7: Append Q&A block 8 to the Q&A file**

---

### Task 9: Wire the filter worker to detection

**Files:**
- Modify: `src/workers/filter.js` (full rewrite of the file from Task 6)
- Create: `test/filter-logic.test.js`
- Create: `src/workers/filter-logic.js`

**Interfaces:**
- Consumes: `fingerprint` from Task 7; `recordOccurrence`, `claimAnalysis`, `pushContext`, `readContext` from Task 8; `config` from Task 2
- Produces: `processLog(deps, log) → Promise<{ action, fingerprint, count }>` where `action` is `'counted'`, `'suppressed'` or `'escalated'`, and `deps` is `{ redis, thresholds: { alertThreshold, windowSeconds, claimTtlSeconds, contextSamples } }`

The decision logic is extracted into `filter-logic.js` so it can be tested without Kafka. `filter.js` becomes a thin Kafka adapter around it.

- [ ] **Step 1: Write the failing test — `test/filter-logic.test.js`**

```js
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { createRedis } from '../src/shared/redis.js';
import { processLog } from '../src/workers/filter-logic.js';

let redis, deps;
const thresholds = { alertThreshold: 3, windowSeconds: 10, claimTtlSeconds: 60, contextSamples: 5 };

beforeAll(() => { redis = createRedis(); deps = { redis, thresholds }; });
afterAll(async () => { await redis.quit(); });
beforeEach(async () => { await redis.flushdb(); });

const log = (message) => ({ service_id: 'payments', level: 'error', message });

describe('processLog', () => {
  it('counts occurrences below the threshold', async () => {
    const first = await processLog(deps, log('db timeout after 10ms'));
    expect(first.action).toBe('counted');
    expect(first.count).toBe(1);
  });

  it('escalates exactly once when the threshold is crossed', async () => {
    await processLog(deps, log('db timeout after 10ms'));
    await processLog(deps, log('db timeout after 20ms'));
    const third = await processLog(deps, log('db timeout after 30ms'));
    expect(third.action).toBe('escalated');
    expect(third.count).toBe(3);

    const fourth = await processLog(deps, log('db timeout after 40ms'));
    expect(fourth.action).toBe('suppressed');
  });

  it('counts structurally identical messages toward one window', async () => {
    // The whole point: varying numbers must not create separate windows,
    // or an error storm never reaches the threshold.
    const a = await processLog(deps, log('db timeout after 1ms'));
    const b = await processLog(deps, log('db timeout after 99999ms'));
    expect(b.fingerprint).toBe(a.fingerprint);
    expect(b.count).toBe(2);
  });

  it('accumulates context samples for escalation', async () => {
    for (let i = 1; i <= 3; i++) await processLog(deps, log(`db timeout after ${i}ms`));
    const result = await processLog(deps, log('db timeout after 4ms'));
    expect(result.action).toBe('suppressed');
    const stored = await redis.lrange(`ctx:${result.fingerprint}`, 0, -1);
    expect(stored.length).toBeGreaterThan(0);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/filter-logic.test.js`
Expected: FAIL — cannot resolve `../src/workers/filter-logic.js`

- [ ] **Step 3: Write `src/workers/filter-logic.js`**

```js
import { fingerprint as computeFingerprint } from '../shared/fingerprint.js';
import { recordOccurrence, claimAnalysis, pushContext, readContext } from '../shared/redis.js';

/**
 * Order is deliberate: every occurrence is counted BEFORE any deduplication.
 * Deduplicating first would collapse an error storm into a single occurrence
 * and the threshold would never be crossed — suppressing exactly the incident
 * we exist to detect. The claim guards the expensive analysis, not the count.
 */
export async function processLog(deps, log) {
  const { redis, thresholds } = deps;
  const { alertThreshold, windowSeconds, claimTtlSeconds, contextSamples } = thresholds;

  const fingerprint = computeFingerprint(log.message);

  await pushContext(redis, fingerprint, log.message, contextSamples, windowSeconds * 6);

  const count = await recordOccurrence(redis, {
    serviceId: log.service_id,
    fingerprint,
    windowSeconds,
  });

  if (count < alertThreshold) {
    return { action: 'counted', fingerprint, count };
  }

  const won = await claimAnalysis(redis, fingerprint, claimTtlSeconds);
  if (!won) {
    return { action: 'suppressed', fingerprint, count };
  }

  const sampleLogs = await readContext(redis, fingerprint);
  return {
    action: 'escalated',
    fingerprint,
    count,
    request: {
      service_id: log.service_id,
      fingerprint,
      occurrence_count: count,
      window_seconds: windowSeconds,
      sample_logs: sampleLogs,
    },
  };
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/filter-logic.test.js`
Expected: PASS — 4 tests

- [ ] **Step 5: Rewrite `src/workers/filter.js` as a Kafka adapter**

```js
import { runConsumer, createProducer, TOPICS, onShutdown } from '../shared/kafka.js';
import { decode, encode } from '../shared/events.js';
import { createRedis } from '../shared/redis.js';
import { config } from '../shared/config.js';
import { logger } from '../shared/logger.js';
import { processLog } from './filter-logic.js';

const redis = createRedis();
const producer = await createProducer('filter-worker');

const deps = {
  redis,
  thresholds: {
    alertThreshold:   config.ALERT_THRESHOLD,
    windowSeconds:    config.WINDOW_SECONDS,
    claimTtlSeconds:  config.CLAIM_TTL_SECONDS,
    contextSamples:   config.CONTEXT_SAMPLES,
  },
};

const consumer = await runConsumer({
  clientId: 'filter-worker',
  groupId: 'filter-workers',
  topics: [TOPICS.RAW_LOGS],
  handler: async ({ partition, message }) => {
    let event;
    try {
      event = decode(message.value);
    } catch (err) {
      // Poison pill: never rethrow. A throw here is retried forever and
      // stalls this partition for every service sharing it.
      logger.error({ err, partition, offset: message.offset }, 'undecodable message dropped');
      return;
    }

    const result = await processLog(deps, event.payload);

    if (result.action === 'escalated') {
      await producer.send({
        topic: TOPICS.ANALYSIS_REQUESTS,
        messages: [{
          key: result.request.service_id,
          value: encode('analysis.requested', result.request),
        }],
      });
      logger.warn({ fingerprint: result.fingerprint, count: result.count }, 'incident escalated');
    } else {
      logger.debug({ action: result.action, count: result.count }, 'log processed');
    }
  },
});

onShutdown(async () => {
  await consumer.disconnect();
  await producer.disconnect();
  await redis.quit();
});
```

- [ ] **Step 6: Verify escalation end to end with a low threshold**

Terminal 1: `npm run start:api`
Terminal 2: `ALERT_THRESHOLD=5 npm run start:filter`
Terminal 3:

```bash
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:3000/v1/logs -H 'Content-Type: application/json' \
    -d "{\"service_id\":\"payments\",\"level\":\"error\",\"message\":\"db timeout after ${i}ms\"}" > /dev/null
done
```

Expected: terminal 2 logs `incident escalated` **exactly once**, not six times. Confirm in Redpanda Console that `ai-analysis-requests` holds exactly one message.

- [ ] **Step 7: Commit**

```bash
git add src/workers/filter.js src/workers/filter-logic.js test/filter-logic.test.js
git commit -m "feat: wire filter worker to fingerprinting, windowing and claiming"
```

- [ ] **Step 8: Append Q&A block 9 to the Q&A file**

---

### Task 10: Postgres access layer

**Files:**
- Create: `src/shared/db.js`, `test/db.test.js`

**Interfaces:**
- Consumes: `config` from Task 2
- Produces:
  - `createPool() → Pool`
  - `insertIncident(pool, incident) → Promise<string>` — returns the generated id. `incident` fields: `service_id`, `fingerprint`, `title`, `summary`, `probable_cause`, `suggested_fix`, `confidence`, `severity`, `occurrence_count`, `window_seconds`, `sample_logs` (array), `llm_model`, `llm_tokens`, `llm_latency_ms`
  - `recentIncidents(pool, limit) → Promise<object[]>` — newest first

- [ ] **Step 1: Write the failing test — `test/db.test.js`**

```js
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { createPool, insertIncident, recentIncidents } from '../src/shared/db.js';

let pool;
beforeAll(() => { pool = createPool(); });
afterAll(async () => { await pool.end(); });
beforeEach(async () => { await pool.query('TRUNCATE incidents'); });

const incident = {
  service_id: 'payments',
  fingerprint: 'a'.repeat(40),
  title: 'Repeated database timeout',
  summary: 'The payments service saw 60 connection timeouts in 10 seconds.',
  probable_cause: 'Connection pool exhaustion',
  suggested_fix: 'Raise pool size or add a circuit breaker',
  confidence: 0.8,
  severity: 'high',
  occurrence_count: 60,
  window_seconds: 10,
  sample_logs: ['db timeout after 10ms', 'db timeout after 20ms'],
  llm_model: 'stub',
  llm_tokens: 0,
  llm_latency_ms: 200,
};

describe('insertIncident', () => {
  it('returns the new row id', async () => {
    const id = await insertIncident(pool, incident);
    expect(id).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('round-trips sample_logs as jsonb', async () => {
    await insertIncident(pool, incident);
    const [row] = await recentIncidents(pool, 10);
    expect(row.sample_logs).toEqual(['db timeout after 10ms', 'db timeout after 20ms']);
  });
});

describe('recentIncidents', () => {
  it('returns newest first and respects the limit', async () => {
    await insertIncident(pool, { ...incident, title: 'first' });
    await insertIncident(pool, { ...incident, title: 'second' });
    const rows = await recentIncidents(pool, 1);
    expect(rows).toHaveLength(1);
    expect(rows[0].title).toBe('second');
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/db.test.js`
Expected: FAIL — cannot resolve `../src/shared/db.js`

- [ ] **Step 3: Write `src/shared/db.js`**

```js
import pg from 'pg';
import { randomUUID } from 'node:crypto';
import { config } from './config.js';

const { Pool } = pg;

// Workers are long-lived processes, so a pool held open for the lifetime of
// the process is correct here. Serverless functions would need a proxy such
// as PgBouncer instead, because each invocation would open its own connection.
export function createPool() {
  return new Pool({ connectionString: config.DATABASE_URL, max: 10 });
}

const INSERT_SQL = `
  INSERT INTO incidents (
    id, service_id, fingerprint, title, summary, probable_cause, suggested_fix,
    confidence, severity, occurrence_count, window_seconds, sample_logs,
    llm_model, llm_tokens, llm_latency_ms
  ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
  RETURNING id
`;

export async function insertIncident(pool, incident) {
  const id = randomUUID();
  const { rows } = await pool.query(INSERT_SQL, [
    id,
    incident.service_id,
    incident.fingerprint,
    incident.title,
    incident.summary,
    incident.probable_cause ?? null,
    incident.suggested_fix ?? null,
    incident.confidence ?? null,
    incident.severity,
    incident.occurrence_count,
    incident.window_seconds,
    JSON.stringify(incident.sample_logs),
    incident.llm_model ?? null,
    incident.llm_tokens ?? null,
    incident.llm_latency_ms ?? null,
  ]);
  return rows[0].id;
}

export async function recentIncidents(pool, limit = 50) {
  const { rows } = await pool.query(
    'SELECT * FROM incidents ORDER BY created_at DESC, id DESC LIMIT $1',
    [limit],
  );
  return rows;
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/db.test.js`
Expected: PASS — 3 tests

- [ ] **Step 5: Commit**

```bash
git add src/shared/db.js test/db.test.js
git commit -m "feat: add postgres incident store"
```

- [ ] **Step 6: Append Q&A block 10 to the Q&A file**

---

### Task 11: Analyzer interface and stub

**Files:**
- Create: `src/workers/analyzers/schema.js`, `src/workers/analyzers/fake.js`, `test/analyzer-fake.test.js`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `diagnosisSchema` — zod schema with fields `title`, `summary`, `probable_cause`, `suggested_fix`, `confidence` (0–1), `severity` (`low`/`medium`/`high`/`critical`), `affected_component`
  - `createFakeAnalyzer({ delayMs? }) → analyze(request) → Promise<{ diagnosis, model, tokens, latencyMs }>` where `request` matches `analysisRequestSchema` from Task 3

Every analyzer — the stub here and the Groq one in the next plan — returns this same shape. That is the seam that lets Weeks 1–2 run with no API key and lets tests stay deterministic.

- [ ] **Step 1: Write the failing test — `test/analyzer-fake.test.js`**

```js
import { describe, it, expect } from 'vitest';
import { createFakeAnalyzer } from '../src/workers/analyzers/fake.js';
import { diagnosisSchema } from '../src/workers/analyzers/schema.js';

const request = {
  service_id: 'payments',
  fingerprint: 'a'.repeat(40),
  occurrence_count: 60,
  window_seconds: 10,
  sample_logs: ['db timeout after 10ms', 'db timeout after 20ms'],
};

describe('fake analyzer', () => {
  it('returns a diagnosis satisfying the shared schema', async () => {
    const analyze = createFakeAnalyzer({ delayMs: 0 });
    const result = await analyze(request);
    expect(() => diagnosisSchema.parse(result.diagnosis)).not.toThrow();
  });

  it('reports model, token and latency metadata', async () => {
    const analyze = createFakeAnalyzer({ delayMs: 0 });
    const result = await analyze(request);
    expect(result.model).toBe('fake-analyzer');
    expect(typeof result.tokens).toBe('number');
    expect(typeof result.latencyMs).toBe('number');
  });

  it('echoes the occurrence count into the summary', async () => {
    const analyze = createFakeAnalyzer({ delayMs: 0 });
    const result = await analyze(request);
    expect(result.diagnosis.summary).toContain('60');
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/analyzer-fake.test.js`
Expected: FAIL — cannot resolve the analyzer modules

- [ ] **Step 3: Write `src/workers/analyzers/schema.js`**

```js
import { z } from 'zod';

// Every analyzer must produce exactly this shape. Structured output, rather
// than free-form markdown, is what makes the result queryable in Postgres and
// gives invalid output a defined failure mode.
export const diagnosisSchema = z.object({
  title:              z.string().min(1).max(200),
  summary:            z.string().min(1).max(4000),
  probable_cause:     z.string().min(1).max(2000),
  suggested_fix:      z.string().min(1).max(2000),
  confidence:         z.number().min(0).max(1),
  severity:           z.enum(['low', 'medium', 'high', 'critical']),
  affected_component: z.string().min(1).max(200),
});
```

- [ ] **Step 4: Write `src/workers/analyzers/fake.js`**

```js
import { diagnosisSchema } from './schema.js';

export function createFakeAnalyzer({ delayMs = 200 } = {}) {
  return async function analyze(request) {
    const started = Date.now();
    if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs));

    const diagnosis = diagnosisSchema.parse({
      title: `Repeated failure in ${request.service_id}`,
      summary: `Observed ${request.occurrence_count} occurrences of one failure pattern in ${request.window_seconds}s.`,
      probable_cause: 'Stub analyzer — no model was called.',
      suggested_fix: 'Stub analyzer — no model was called.',
      confidence: 0.5,
      severity: 'medium',
      affected_component: request.service_id,
    });

    return { diagnosis, model: 'fake-analyzer', tokens: 0, latencyMs: Date.now() - started };
  };
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `npx vitest run test/analyzer-fake.test.js`
Expected: PASS — 3 tests

- [ ] **Step 6: Commit**

```bash
git add src/workers/analyzers test/analyzer-fake.test.js
git commit -m "feat: add analyzer interface and stub implementation"
```

- [ ] **Step 7: Append Q&A block 11 to the Q&A file**

---

### Task 12: AI worker — Week 2 milestone

**Files:**
- Create: `src/workers/ai.js`, `src/workers/ai-logic.js`, `test/ai-logic.test.js`
- Modify: `package.json` (add `start:ai` script)

**Interfaces:**
- Consumes: `decode` from Task 3; `runConsumer`, `TOPICS`, `onShutdown` from Task 4; `insertIncident`, `createPool` from Task 10; `createFakeAnalyzer` from Task 11; `createRedis`, `releaseClaim` from Task 8
- Produces: `diagnose({ analyze, pool }, request) → Promise<{ incidentId }>`

- [ ] **Step 1: Write the failing test — `test/ai-logic.test.js`**

```js
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { createPool, recentIncidents } from '../src/shared/db.js';
import { createFakeAnalyzer } from '../src/workers/analyzers/fake.js';
import { diagnose } from '../src/workers/ai-logic.js';

let pool, deps;
beforeAll(() => {
  pool = createPool();
  deps = { pool, analyze: createFakeAnalyzer({ delayMs: 0 }) };
});
afterAll(async () => { await pool.end(); });
beforeEach(async () => { await pool.query('TRUNCATE incidents'); });

const request = {
  service_id: 'payments',
  fingerprint: 'b'.repeat(40),
  occurrence_count: 60,
  window_seconds: 10,
  sample_logs: ['db timeout after 10ms'],
};

describe('diagnose', () => {
  it('persists an incident and returns its id', async () => {
    const { incidentId } = await diagnose(deps, request);
    expect(incidentId).toMatch(/^[0-9a-f-]{36}$/);

    const [row] = await recentIncidents(pool, 1);
    expect(row.service_id).toBe('payments');
    expect(row.occurrence_count).toBe(60);
    expect(row.severity).toBe('medium');
  });

  it('records analyzer metadata on the row', async () => {
    await diagnose(deps, request);
    const [row] = await recentIncidents(pool, 1);
    expect(row.llm_model).toBe('fake-analyzer');
    expect(row.llm_latency_ms).toBeGreaterThanOrEqual(0);
  });

  it('rejects a diagnosis that fails the schema', async () => {
    const badDeps = { pool, analyze: async () => ({
      diagnosis: { title: 'x' }, model: 'broken', tokens: 0, latencyMs: 1,
    }) };
    await expect(diagnose(badDeps, request)).rejects.toThrow();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/ai-logic.test.js`
Expected: FAIL — cannot resolve `../src/workers/ai-logic.js`

- [ ] **Step 3: Write `src/workers/ai-logic.js`**

```js
import { insertIncident } from '../shared/db.js';
import { diagnosisSchema } from './analyzers/schema.js';

export async function diagnose({ analyze, pool }, request) {
  const result = await analyze(request);

  // Validate on the way out as well as inside the analyzer. The next plan
  // swaps in a real model whose output cannot be trusted to hold its shape.
  const diagnosis = diagnosisSchema.parse(result.diagnosis);

  const incidentId = await insertIncident(pool, {
    service_id:       request.service_id,
    fingerprint:      request.fingerprint,
    title:            diagnosis.title,
    summary:          diagnosis.summary,
    probable_cause:   diagnosis.probable_cause,
    suggested_fix:    diagnosis.suggested_fix,
    confidence:       diagnosis.confidence,
    severity:         diagnosis.severity,
    occurrence_count: request.occurrence_count,
    window_seconds:   request.window_seconds,
    sample_logs:      request.sample_logs,
    llm_model:        result.model,
    llm_tokens:       result.tokens,
    llm_latency_ms:   result.latencyMs,
  });

  return { incidentId };
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/ai-logic.test.js`
Expected: PASS — 3 tests

- [ ] **Step 5: Write `src/workers/ai.js`**

```js
import { runConsumer, TOPICS, onShutdown } from '../shared/kafka.js';
import { decode } from '../shared/events.js';
import { createPool } from '../shared/db.js';
import { createRedis, releaseClaim } from '../shared/redis.js';
import { createFakeAnalyzer } from './analyzers/fake.js';
import { logger } from '../shared/logger.js';
import { diagnose } from './ai-logic.js';

const pool = createPool();
const redis = createRedis();
const deps = { pool, analyze: createFakeAnalyzer({ delayMs: 200 }) };

const consumer = await runConsumer({
  clientId: 'ai-worker',
  groupId: 'ai-workers',
  topics: [TOPICS.ANALYSIS_REQUESTS],
  handler: async ({ partition, message }) => {
    let event;
    try {
      event = decode(message.value);
    } catch (err) {
      logger.error({ err, partition, offset: message.offset }, 'undecodable request dropped');
      return;
    }

    try {
      const { incidentId } = await diagnose(deps, event.payload);
      logger.info({ incidentId, service: event.payload.service_id }, 'incident diagnosed');
    } catch (err) {
      // Release the claim so a future occurrence can retry this fingerprint.
      // Without this, a failure here would silence the error for the full
      // claim TTL. The dead-letter path arrives in the next plan.
      await releaseClaim(redis, event.payload.fingerprint);
      logger.error({ err, fingerprint: event.payload.fingerprint }, 'diagnosis failed, claim released');
    }
  },
});

onShutdown(async () => {
  await consumer.disconnect();
  await redis.quit();
  await pool.end();
});
```

- [ ] **Step 6: Add the start script**

```bash
npm pkg set scripts.start:ai="node src/workers/ai.js"
```

- [ ] **Step 7: Verify the Week 2 milestone end to end**

Terminal 1: `npm run start:api`
Terminal 2: `ALERT_THRESHOLD=50 npm run start:filter`
Terminal 3: `npm run start:ai`
Terminal 4:

```bash
for i in $(seq 1 60); do
  curl -s -X POST http://localhost:3000/v1/logs -H 'Content-Type: application/json' \
    -d "{\"service_id\":\"payments\",\"level\":\"error\",\"message\":\"db connection timeout after ${i}ms\"}" > /dev/null
done
```

Expected: terminal 2 logs `incident escalated` once; terminal 3 logs `incident diagnosed` once.

- [ ] **Step 8: Confirm the row landed in Postgres**

```bash
docker compose exec postgres psql -U aether -d aetherinsight \
  -c "SELECT service_id, severity, occurrence_count, llm_model, created_at FROM incidents;"
```

Expected: exactly one row — `payments`, `medium`, `60`, `fake-analyzer`.

- [ ] **Step 9: Run the whole suite**

Run: `npm test`
Expected: PASS — all tests across all files.

- [ ] **Step 10: Commit**

```bash
git add src/workers/ai.js src/workers/ai-logic.js test/ai-logic.test.js package.json
git commit -m "feat: add ai worker persisting diagnosed incidents"
```

- [ ] **Step 11: Append Q&A block 12 to the Q&A file**

---

## Self-Review

**Spec coverage.** Sections 3.1 (layout), 3.2 (topics), 3.3 (envelope), 4.1–4.5 (filter worker), 5.1 (structured output), 6 (persistence), and the Week 1 and Week 2 rows of section 11 are each implemented by a task above. Section 5.2 (circuit breaker, budget cap, DLQ), 7 (dashboard), 8 (observability), and the integration/chaos/load parts of 10 are Weeks 3–5 and belong to the next plans — this is intentional, not a gap.

**Placeholder scan.** No TBDs; every code step contains complete runnable code; every test step contains real assertions.

**Type consistency.** `fingerprint()` returns 40 hex chars, matched by `analysisRequestSchema.fingerprint.length(40)` in Task 3 and the `'a'.repeat(40)` fixtures in Tasks 10–12. `processLog` returns `{ action, fingerprint, count, request? }`, consumed unchanged by `filter.js` in Task 9. Analyzer return shape `{ diagnosis, model, tokens, latencyMs }` is produced in Task 11 and consumed in Task 12. `insertIncident` parameter names match the `incidents` column names in `schema.sql`.

---

## Q&A Blocks

Append each block to `D:\Interview_material_NR\Claude Project QnA.txt` at the end of its task. The file is a running log — append only.

**Block 1 (Task 1) — Infrastructure.** Why Redpanda over Apache Kafka; what a broker is; why healthchecks matter in Compose; why the schema is mounted into `docker-entrypoint-initdb.d`.

**Block 2 (Task 2) — Config.** Why config is validated at startup rather than read ad hoc; fail-fast versus fail-late.

**Block 3 (Task 3) — Schema evolution.** What the `v` field buys; how you would roll out a breaking payload change with consumers running.

**Block 4 (Task 4) — Kafka fundamentals.** Partitions, keys and ordering guarantees; consumer groups; why auto topic creation is disabled; what a rebalance is.

**Block 5 (Task 5) — Ingestion.** Why 202 rather than 200; `acks` durability trade-offs; what backpressure means at the HTTP layer.

**Block 6 (Task 6) — Consuming.** Why a throw inside `eachMessage` stalls a partition; what graceful shutdown must do and in what order.

**Block 7 (Task 7) — Fingerprinting.** Why hashing raw text fails; what Drain3 does; why rule order matters.

**Block 8 (Task 8) — Redis.** Sorted sets as sliding windows; why the script must be atomic; `SET NX EX` as a lock primitive; why TTLs are the recovery mechanism.

**Block 9 (Task 9) — The ordering decision.** Why counting precedes deduplication; what breaks if reversed; why the claim, not the count, is what gets deduplicated.

**Block 10 (Task 10) — Postgres.** Connection pooling in long-lived versus serverless processes; why parameterized queries; why no ORM.

**Block 11 (Task 11) — The seam.** Why the fake analyzer is built first; how an interface boundary keeps tests deterministic and CI free.

**Block 12 (Task 12) — End to end.** Trace one log line through every hop; where duplicates can occur; why at-least-once plus a claim yields effectively-once analysis.

Full question-and-answer text for each block is written into the Q&A file as its task completes.
