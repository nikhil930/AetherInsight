# AetherInsight — Session Context Brief

**Purpose of this file:** any fresh Claude session working on AetherInsight
should read this first. It captures what the project is, what has been built,
what is still pending, which decisions are locked in, and the ground rules
the user has set.

Last updated: 2026-08-13 (Task 11 complete — analyzer interface + createFakeAnalyzer built, 50/50 tests green; Gemini env vars wired into config).

---

## 1. What we are building

AetherInsight is a **real-time distributed log analyzer with async LLM
root-cause analysis**. Services push logs; the system detects storms of
structurally-similar errors and asks an LLM to produce a diagnosed incident
(summary, probable cause, suggested fix). A dashboard shows incidents live.

**This is a portfolio / interview project**, not a shipped product. The user
is Nikhil Raghuvanshi, a **beginner** at Kafka, Redis, Docker, and Node.
Every step needs (a) *why this choice*, (b) a small syntax primer for the
tool in play, and (c) interviewer-style Q&A appended to a running log.

**Budget:** ~10 hrs/week, 4–6 weeks. Weeks 1–2 = foundation (Kafka + API +
filter worker + AI worker); Weeks 3–5 = dashboard, persistence hardening,
observability, testing, chaos.

---

## 2. Architecture in one paragraph

Producer services POST logs to a **Fastify ingestion API** which validates
and produces to a **Redpanda topic (`raw-logs`) keyed by `service_id`**.
A **filter worker** consumes `raw-logs`, fingerprints each message (variable
substrings → `<*>` then sha1), and maintains a **per-service sliding window
in Redis (ZSET)**. When the count in the window crosses `ALERT_THRESHOLD`,
the worker attempts an **idempotency claim in Redis** (`SET NX EX`) for the
fingerprint. If the claim wins, it produces an `analysis.requested` event
to `ai-analysis-requests`. An **AI worker** consumes that topic, calls the
LLM (structured JSON out), writes the diagnosed incident to **Postgres**,
and publishes to `diagnosed-incidents`. The dashboard's WebSocket layer
fans that out to browsers.

**The critical ordering rule:** count BEFORE deduplicating. Deduplication
guards the expensive LLM call — not the counting. Reversing this would
suppress the exact storms the system exists to detect.

---

## 3. Stack (versions)

| Layer | Choice | Version | Why |
|---|---|---|---|
| Runtime | Node.js | 22 (ESM) | Modern async, top-level await |
| HTTP | Fastify | 4.28 | Faster than Express, `app.inject()` for tests |
| Broker | **Redpanda** | 24.2.7 | Kafka wire-compatible, no JVM/ZK, ~2s startup |
| Kafka client | KafkaJS | 2.2 | Standard JS Kafka client |
| Cache/state | Redis | 7-alpine | Sliding windows, idempotency claims |
| DB | Postgres | 16-alpine | Source of truth for incidents |
| Postgres driver | `pg` | 8.12 | **No ORM** — hand-written SQL |
| Schema | zod | 3.23 | Config + payload validation |
| Logger | pino + pino-pretty | 9.4 | Structured JSON in prod, human in dev |
| Tests | vitest | 2.1 | ESM-native, fast |

**No Redlock. No Supabase. No ORM.** See §7 for reasoning.

---

## 4. What is built (verified)

| # | Task | Files | Tests | Verified |
|---|---|---|---|---|
| 1 | Scaffold + Compose + schema | `docker-compose.yml`, `schema.sql`, `package.json`, `.gitignore`, src/test dirs | – | Stack up: `docker compose ps` shows Redpanda + Redis + Postgres healthy |
| 2 | Config (zod, fail-fast) | `src/shared/config.js`, `src/shared/logger.js` | 3/3 | ✅ |
| 3 | Event envelope (v/type/id/ts/payload) | `src/shared/events.js` | 5/5 | ✅ |
| 4 | Kafka helpers + topic script | `src/shared/kafka.js`, `scripts/create-topics.js` | – | ✅ 4 topics created & idempotent |
| 5 | Ingestion API | `src/api/server.js` | 7/7 | ✅ Real message round-tripped through broker |
| 6 | Filter worker skeleton (Week 1 milestone) | `src/workers/filter.js` | – | ✅ Live consumer decodes and logs; deterministic partition keying verified |
| 7 | Fingerprint (normalize + sha1) | `src/shared/fingerprint.js` | 9/9 | ✅ UUID/IPv4/hex/quoted/number/whitespace rules; ordering trap covered by tests |
| 8 | Redis: sliding window (ZSET+Lua) / claim (`SET NX EX`) / context buffer (LPUSH+LTRIM) | `src/shared/redis.js`, `scripts/seed-redis-inspect.mjs` | 6/6 | ✅ Verified against live Redis: eviction, per-service isolation, claim once-only, LTRIM cap; keys inspected via `redis-cli` |
| 9 | Filter worker wired to detection (pure `filter-logic.js` + thin Kafka adapter `filter.js`) | `src/workers/filter-logic.js`, `src/workers/filter.js` (rewritten), `test/filter-logic.test.js`, `vitest.config.js` | 4/4 | ✅ End-to-end: 10 same-shape logs → exactly 1 `incident escalated` line, exactly 1 message on `ai-analysis-requests` topic with correct fingerprint + samples |
| 10 | Postgres access layer (pool + insertIncident + getIncident + listIncidents, hand-written SQL, no ORM) | `src/shared/db.js`, `test/db.test.js` | 6/6 | ✅ Real-Postgres tests: round-trip all columns incl. jsonb `sample_logs`, `ON CONFLICT (id) DO NOTHING` idempotency (same id twice = 1 row), listIncidents newest-first ordering, service_id filter, pagination |
| 11 | Analyzer interface + fake (ports-and-adapters seam for the AI worker) | `src/shared/analyzer.js`, `test/analyzer.test.js` | 10/10 | ✅ Deterministic output per fingerprint, latency injection via `delayMs`, failure injection via `failEvery`, AbortSignal cancellation, buildPrompt caps samples at 5, zod boundary validation on both request and response; Gemini env vars (`GEMINI_API_KEY` optional, `GEMINI_MODEL` default `gemini-2.0-flash`) added to config |

**Total tests: 50/50 passing.** Filter worker (`filter.js`) has no test file by design
(see Block 6 Q5 in the Q&A log — I/O adapter, no pure logic yet; Task 9
will extract `filter-logic.js` for testing).

**Topics on the broker right now:**
- `raw-logs` — 3 partitions, keyed by service_id
- `ai-analysis-requests` — 3 partitions, keyed by service_id
- `diagnosed-incidents` — 1 partition (global order for dashboard)
- `dead-letter` — 1 partition (chronological for debugging)

**Postgres tables on first boot:** `incidents`, `dead_letters`.

---

## 5. What is pending

| # | Task | Status |
|---|---|---|
| 12 | AI worker (Week 2 milestone) | **Next up.** Consumes `ai-analysis-requests`, calls `analyzer.analyze()` (injected — fake in tests, real Gemini in prod), writes result via `insertIncident`, produces to `diagnosed-incidents`. |
| 12b | Real Gemini adapter | After 12 — thin `createGeminiAnalyzer({apiKey, model})` implementing the same `analyze(request)` port using `@google/generative-ai` with structured-JSON responseMimeType. |
| ⭐ FINAL | README polish + proof-of-working screenshots | **End-of-project checklist.** Rewrite README with: architecture diagram (use `docs/design-artifact.html` as hero), quickstart, end-to-end demo screenshots (log → escalation → LLM response → dashboard). Screenshots go at the **end** of the README as visual proof. Do NOT skip — this is the interviewer's first impression of the whole build. |

**Also open, not blocking:** Redpanda Console UI container (`aether-console`,
port 8080) fails to start on v2.7.2 with `unable to find user redpandaconsole`.
Cosmetic (topic browser only). Fix by pinning v2.6.1 or adding `user: root`.

---

## 6. Key file paths

```
D:\Projects\AetherInsight\
├── docker-compose.yml
├── schema.sql
├── package.json
├── .gitignore
├── docs\
│   ├── CONTEXT.md                    ← this file
│   ├── FLOWS_AND_TRADEOFFS.md        ← revision doc: every choice, every trade-off, interview probe map
│   └── superpowers\
│       ├── specs\2026-08-03-aetherinsight-design.md   ← the design doc
│       └── plans\2026-08-03-aetherinsight-foundation.md ← the active plan (Weeks 1–2)
├── src\
│   ├── shared\
│   │   ├── config.js         ← zod-validated env
│   │   ├── logger.js         ← pino
│   │   ├── events.js         ← envelope + rawLogSchema + analysisRequestSchema
│   │   ├── kafka.js          ← createKafka / createProducer / runConsumer / onShutdown / TOPICS
│   │   ├── fingerprint.js    ← normalize + sha1 (6 ordered rules, 9/9 tests green)
│   │   ├── redis.js          ← createRedis / recordOccurrence (Lua ZSET) / claimAnalysis (SET NX EX) / pushContext (LPUSH+LTRIM) / readContext (6/6 tests green)
│   │   ├── db.js             ← createDb (pg.Pool) / insertIncident (ON CONFLICT DO NOTHING) / getIncident / listIncidents — flat columns, no ORM (6/6 tests green)
│   │   └── analyzer.js       ← analyzeRequestSchema / analysisSchema / buildPrompt / createFakeAnalyzer({delayMs, failEvery}) — port for LLM adapter, fake for tests (10/10 tests green)
│   ├── api\
│   │   └── server.js         ← Fastify app factory + POST /ingest + GET /healthz
│   └── workers\
│       ├── filter.js         ← thin Kafka adapter: consume raw-logs → processLog → produce to ai-analysis-requests on escalate
│       └── filter-logic.js   ← pure decision logic: fingerprint + count + claim; testable without Kafka (4/4 tests)
├── scripts\
│   ├── create-topics.js
│   └── seed-redis-inspect.mjs  ← seeds win/claim/ctx keys for hand-inspection via redis-cli
└── test\
    ├── config.test.js
    ├── events.test.js
    ├── kafka helpers have no test — verified via smoke test
    ├── api.test.js
    ├── fingerprint.test.js   ← 9/9 passing
    ├── redis.test.js         ← 6/6 passing (real Redis, injected timestamps for eviction test)
    ├── filter-logic.test.js  ← 4/4 passing (real Redis, no Kafka; escalate/suppress/counted paths)
    ├── db.test.js            ← 6/6 passing (real Postgres, TRUNCATE per test; round-trip + idempotency + pagination)
    └── analyzer.test.js      ← 10/10 passing (fake analyzer: determinism, latency/failure injection, AbortSignal, buildPrompt sample cap, zod boundary checks)

vitest.config.js               ← fileParallelism: false — serialize test files so real-Redis flushdb doesn't race across files

D:\Interview_material_NR\Claude Project QnA.txt
    ← append-only interview Q&A log, currently 1772 lines, Blocks 0–10 done
```

---

## 7. Locked-in design decisions (do not re-litigate)

1. **Redpanda instead of Apache Kafka.** Wire-compatible (KafkaJS unchanged),
   single C++ binary, no JVM, no ZooKeeper. Chosen for dev-UX, not throughput.

2. **Partition by `service_id`.** Guarantees single-writer-per-service in the
   filter worker via Kafka's partition-ownership rule. Removes the need for
   a distributed lock. Also preserves per-service ordering.

3. **Redlock was in the original design; removed.** Kafka partition ownership
   already provides mutual exclusion. Redlock would have been redundant
   infrastructure.

4. **Count-before-dedup.** The original doc had dedup gating the count. That
   would suppress the very storms the system detects. Reordered: always
   count; dedup only guards the expensive LLM call via `SET NX EX`.

5. **Plain Postgres, not Supabase.** Supabase IS Postgres, but Supabase
   Realtime would replace the WebSocket layer we are deliberately building.

6. **No ORM.** Hand-written SQL with `pg` driver. Portfolio value is in
   understanding the queries, not in demonstrating Prisma/Drizzle.

7. **Envelope everywhere.** All messages on all topics carry `{v, type, id,
   ts, payload}`. `v` enables schema evolution; `id` enables idempotency;
   `type` enables dispatch to per-payload schemas.

8. **Fail-fast config, reject-and-continue payloads.** Config uses `parse`
   at boot (crash on invalid). API uses `safeParse` per request (400 on
   invalid, keep serving).

9. **Dependency injection for external resources.** `createApp({ producer })`
   takes the producer as an arg. Tests pass a fake; prod passes the real
   KafkaJS producer. No singleton imports, no NODE_ENV branching.

10. **202 Accepted, not 200/201.** /ingest hands off to Kafka; work is not
    complete. 202 is the correct semantic for queued work.

---

## 8. Ground rules the user has set

1. **User handles ALL git operations.** Do not run `git init`, `git add`,
   `git commit`, `git push`, `git branch`, or anything else that mutates git
   state. Suggest commit messages and mention natural commit points; the
   user runs them.

2. **Repo is at https://github.com/nikhil930/AetherInsight** — user pushes
   incrementally.

3. **The Q&A file is append-only.** `D:\Interview_material_NR\Claude Project QnA.txt`.
   Never rewrite, never reorder. New blocks go at the end.

4. **After every completed task, append a new Q&A block** with the
   interviewer questions and answers for that task. Number blocks
   sequentially (Block 0 = architecture; Blocks 1–N = one per task in
   plan order).

5. **Every step needs three things:**
   - Why this specific choice was made (design rationale)
   - A short syntax primer for the tool being used (Kafka admin API,
     Redis ZADD, etc.)
   - Interviewer Q&A appended to the log

6. **Never paste secrets in chat.** No PATs, no API keys. Environment
   variables set via OS/`.env`, never printed.

7. **Inline execution, not subagent execution.** The user watches every
   decision. Do not delegate task execution to background agents unless
   explicitly asked.

---

## 9. How to run things

**From `D:\Projects\AetherInsight`:**

```bash
# Bring up the data plane
docker compose up -d

# Confirm services are healthy
docker compose ps

# Create Kafka topics (idempotent)
npm run topics

# Run all tests
npx vitest run

# Run one test file
npx vitest run test/api.test.js

# Watch tests
npm run test:watch

# Start the ingestion API (dev)
node src/api/server.js

# List topics from the broker
docker exec aether-redpanda rpk topic list

# Consume from a topic
docker exec aether-redpanda rpk topic consume raw-logs -n 5 -o start

# Ping Redis
docker exec aether-redis redis-cli PING

# Inspect Postgres
docker exec aether-postgres psql -U aether -d aetherinsight -c "\dt"

# Nuke everything (including data volumes)
docker compose down -v
```

**Port note:** the Postgres container maps to host port **5434** (not 5432).
This machine already has native postgresql-x64-15 and postgresql-x64-17
services holding 5432 + 5433, so our container uses 5434 to avoid the
conflict silently routing to the wrong DB. From host tools (pgAdmin,
DBeaver, `psql` on the Windows path), connect with:

```
Host: localhost   Port: 5434   User: aether   Password: aether   DB: aetherinsight
```

`docker exec aether-postgres psql -U aether -d aetherinsight` still works
because it uses the container-internal socket, unaffected by the mapped
port.

---

## 10. Q&A log structure

**File:** `D:\Interview_material_NR\Claude Project QnA.txt` — 2010 lines
as of 2026-08-13. Append-only.

**Blocks written:**
- **Block 0** — Architecture and design decisions (10 Q&As). Written before
  implementation started.
- **Block 1** — Infrastructure, Docker Compose, why Redpanda (6 Q&As)
- **Block 2** — Config, fail-fast validation, zod (7 Q&As)
- **Block 3** — Event envelope and schema evolution (8 Q&As)
- **Block 4** — Kafka fundamentals, partitioning, admin client (10 Q&As)
- **Block 5** — Ingestion API, 202 semantics, dependency injection (9 Q&As)
- **Block 6** — Filter worker, consumer groups, poison pills, graceful
  shutdown, pino field-name collision (9 Q&As)
- **Block 7** — Fingerprinting, Drain trade-offs, why sha1, regex ordering,
  static-rule failure modes (10 Q&As)
- **Block 8** — Redis primitives: ZSET vs INCR, Lua vs MULTI, defineCommand,
  KEYS/ARGV convention, randomUUID member trick, SET NX EX vs Redlock,
  LPUSH/LTRIM/EXPIRE ordering, real-Redis vs mocks (10 Q&As)
- **Block 9** — Filter worker wiring: ports-and-adapters, count-before-dedup
  ordering rule, pushContext-before-recordOccurrence, deps injection,
  escalated payload shape, vitest fileParallelism race, Kafka partition
  ownership as mutex, plan-vs-code drift (10 Q&As)
- **Block 10** — Postgres access: pool vs client, parameterized queries,
  `ON CONFLICT DO NOTHING` idempotency, flat vs JSONB, no-ORM revisit,
  real-Postgres tests, jsonb round-trip, prepared statements, schema.sql
  as init seed (10 Q&As)
- **Block 11** — Analyzer port + fake adapter: ports-and-adapters pattern,
  fakes vs mocks (Fowler), factory functions for per-instance state,
  deterministic output from fingerprint, AbortSignal, buildPrompt as
  separate helper, sample cap defense-in-depth, boundary validation,
  optional GEMINI_API_KEY, failEvery vs failRate (10 Q&As)

**Blocks planned:**
- Block 12 — End-to-end trace, backpressure, observability

---

## 11. What the user's global memory already knows

Loaded automatically on every session:

- `user-learning-distributed-systems.md` — beginner at Kafka/Redis/Docker/Node
- `explain-concepts-with-syntax-primers.md` — every step needs
  why-this-choice + syntax primer + interviewer Q&A
- `interview-qna-log.md` — the log path is
  `D:\Interview_material_NR\Claude Project QnA.txt`, append-only
- `aetherinsight-project.md` — project goal, 5-week budget, key scope
  decisions

The user's memory is the durable truth. This file is the point-in-time
build state. Both are needed.
