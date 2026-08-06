# AetherInsight — Session Context Brief

**Purpose of this file:** any fresh Claude session working on AetherInsight
should read this first. It captures what the project is, what has been built,
what is still pending, which decisions are locked in, and the ground rules
the user has set.

Last updated: 2026-08-05 (during Task 5 completion).

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

**Total tests: 15/15 passing** on the code that is complete. `fingerprint.test.js`
has 9 tests currently failing — see §5.

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
| 6 | Filter worker skeleton (Week 1 milestone) | Not started |
| 7 | Fingerprinting (`src/shared/fingerprint.js`) | **User is filling in the RULES array.** Test file complete (9 tests defining the contract). |
| 8 | Redis sliding window / idempotency claim / context buffer | Not started |
| 9 | Wire filter worker to detection | Not started |
| 10 | Postgres access layer | Not started |
| 11 | Analyzer interface + fake | Not started |
| 12 | AI worker (Week 2 milestone) | Not started |

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
│   └── superpowers\
│       ├── specs\2026-08-03-aetherinsight-design.md   ← the design doc
│       └── plans\2026-08-03-aetherinsight-foundation.md ← the active plan (Weeks 1–2)
├── src\
│   ├── shared\
│   │   ├── config.js         ← zod-validated env
│   │   ├── logger.js         ← pino
│   │   ├── events.js         ← envelope + rawLogSchema + analysisRequestSchema
│   │   ├── kafka.js          ← createKafka / createProducer / runConsumer / onShutdown / TOPICS
│   │   └── fingerprint.js    ← normalize + fingerprint (RULES array TODO)
│   └── api\
│       └── server.js         ← Fastify app factory + POST /ingest + GET /healthz
├── scripts\
│   └── create-topics.js
└── test\
    ├── config.test.js
    ├── events.test.js
    ├── kafka helpers have no test — verified via smoke test
    ├── api.test.js
    └── fingerprint.test.js   ← 9 tests, waiting on RULES

D:\Interview_material_NR\Claude Project QnA.txt
    ← append-only interview Q&A log, currently 1058 lines, Blocks 0–5 done
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

---

## 10. Q&A log structure

**File:** `D:\Interview_material_NR\Claude Project QnA.txt` — 1058 lines
as of 2026-08-05. Append-only.

**Blocks written:**
- **Block 0** — Architecture and design decisions (10 Q&As). Written before
  implementation started.
- **Block 1** — Infrastructure, Docker Compose, why Redpanda (6 Q&As)
- **Block 2** — Config, fail-fast validation, zod (7 Q&As)
- **Block 3** — Event envelope and schema evolution (8 Q&As)
- **Block 4** — Kafka fundamentals, partitioning, admin client (10 Q&As)
- **Block 5** — Ingestion API, 202 semantics, dependency injection (9 Q&As)

**Blocks planned:**
- Block 6 — Filter worker: consuming, poison pills, graceful shutdown
- Block 7 — Fingerprinting / Drain3-style templating
- Block 8 — Redis: ZSET sliding window, `SET NX EX`, TTL as recovery
- Block 9 — The count-before-dedup ordering decision
- Block 10 — Postgres access, connection pooling
- Block 11 — The fake-analyzer seam (testability without LLM)
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
