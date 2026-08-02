# AetherInsight — Design Spec

**Date:** 2026-08-03
**Status:** Approved
**Goal:** Interview / portfolio project. Optimize for concepts the author can defend verbally, not for product completeness.
**Budget:** ~10 hrs/week × 5 weeks, week 6 as buffer.

---

## 1. Problem

Application logs arrive faster than any synchronous handler can process them. AetherInsight ingests high-throughput logs, detects when a specific failure mode is escalating, and asynchronously asks an LLM to produce a structured root-cause analysis, streaming results to a live dashboard.

The distributed-systems plumbing is the substance. The LLM is one component, not the point.

## 2. Scope decisions

**In scope:** ingestion API, fingerprint-based deduplication, sliding-window anomaly detection, async LLM diagnosis with structured output, durable incident storage, WebSocket dashboard, dead-letter queue, Prometheus metrics, integration + chaos tests, load benchmarks.

**Explicitly deferred to a README roadmap:** pgvector RAG over past incidents, full Drain3 templating, multi-tenancy and API keys, tiered retry topics, log-shipping SDK.

**Removed from the original concept, with reasons:**

| Removed | Reason |
|---|---|
| Redlock distributed locking | Kafka partition ownership already guarantees a single writer per `service_id`. The lock was redundant. |
| Four independently deployed microservices | One repo with a shared core and four process entry points. Same architecture, a quarter of the overhead. |
| React dashboard | Demonstrates nothing about distributed systems. One HTML file with a vanilla WebSocket. |
| Ollama / local inference | Groq API. Local inference costs a weekend and produces worse output. |
| Apache Kafka | Redpanda — same wire protocol, no JVM, no Zookeeper. |
| Supabase | Its Realtime engine would replace the WebSocket fan-out layer that is deliberately being built. |

## 3. Architecture

Four Node processes over a shared core, all infrastructure in Docker Compose.

```
POST /v1/logs                                    (Fastify ingest API)
      │  produce
      ▼
 [raw-logs]  3 partitions, key = service_id
      │  consume, group=filter-workers
      ▼
 Filter worker ── fingerprint ── Redis sliding window ── claim
      │  produce (only on threshold breach + claim won)
      ▼
 [ai-analysis-requests]  3 partitions, key = service_id
      │  consume, group=ai-workers
      ▼
 AI worker ── Groq (structured JSON) ── Postgres write
      │  produce                              │  on terminal failure
      ▼                                       ▼
 [diagnosed-incidents]                   [dead-letter]
      │  consume, group unique per instance
      ▼
 Dashboard ── WebSocket ── browser
```

### 3.1 Repository layout

```
aetherinsight/
  docker-compose.yml        redpanda, redis, postgres, prometheus, grafana
  schema.sql
  src/
    shared/
      config.js             env parsing validated with zod
      kafka.js              client factory + consumer runner with graceful shutdown
      redis.js  db.js  logger.js  metrics.js
      events.js             zod schema per event type + versioned envelope
    api/server.js
    workers/filter.js
    workers/ai.js
    dashboard/server.js  dashboard/public/index.html
  test/  bench/
```

`shared/` is what makes four entry points affordable inside the time budget: connection handling, metrics, shutdown and validation exist once.

### 3.2 Topics

| Topic | Partitions | Key | Rationale |
|---|---|---|---|
| `raw-logs` | 3 | `service_id` | Order per service; 3 is enough to demonstrate a rebalance on a laptop |
| `ai-analysis-requests` | 3 | `service_id` | Preserves single-writer-per-service downstream |
| `diagnosed-incidents` | 1 | `incident_id` | Low volume |
| `dead-letter` | 1 | — | Inspection queue |

### 3.3 Event envelope

```js
{ v: 1, type: "log.raw", id: "<uuid>", ts: "<iso8601>", payload: { … } }
```

The `v` field is the schema-evolution story without running a schema registry: consumers branch on `v`, producers bump it, both versions run for one deploy cycle, then the old branch is dropped.

`log.raw` payload: `{ service_id, level, message, stack?, trace_id?, meta? }`.

## 4. Filter worker

Processing order per message — **counting happens before deduplication**:

1. Compute `fingerprint = sha1(normalize(message))`.
2. **Always** record the occurrence in the sliding window.
3. If the window count crosses the threshold, attempt to claim the analysis.
4. Only the claim winner produces to `ai-analysis-requests`.

### 4.1 Why counting precedes deduplication

Deduplicating first suppresses the incident it is meant to detect: a storm of identical errors collapses to a single occurrence and never crosses the threshold. Deduplication protects the **expensive LLM call**, not the counting.

### 4.2 Fingerprinting

Normalize variable substrings before hashing — numbers, UUIDs, hex strings, quoted literals, IPs, file paths — each replaced with `<*>`.

```
"Connection to db-7 timed out after 3021ms for user 4471"
  → "Connection to db-<*> timed out after <*>ms for user <*>"
```

A hand-rolled approximation of the Drain3 log-templating algorithm. Raw hashing would treat two instances of the same failure as unrelated events.

### 4.3 Sliding window

One Lua script per message, keyed `win:{service_id}:{fingerprint}`:

```lua
redis.call('ZREMRANGEBYSCORE', key, 0, now - windowMs)
redis.call('ZADD', key, now, occurrenceId)
redis.call('EXPIRE', key, windowSec)
return redis.call('ZCARD', key)
```

Lua is required for correctness, not just speed: as separate round trips, two workers can both read 49, both increment, and both observe 50 — producing two LLM calls for one incident. Server-side execution makes the read-modify-check atomic. It is also one round trip instead of four.

Keyed per fingerprint rather than per service so an incident describes one failure mode.

### 4.4 Claiming

`SET claim:{fingerprint} inflight NX EX 900`. The winner proceeds; others increment a `suppressed_total` metric. The TTL is the recovery path — if the AI worker dies, the claim expires and the next occurrence retries.

### 4.5 Context buffer

`LPUSH ctx:{fingerprint}` → `LTRIM ctx:{fingerprint} 0 19` → `EXPIRE`. Twenty recent samples for the LLM prompt, self-trimming, no cleanup job.

## 5. AI worker

### 5.1 Structured output

The LLM returns JSON validated with zod, not markdown:

```js
{ title, summary, probable_cause, suggested_fix,
  confidence: 0..1, severity: "low"|"medium"|"high"|"critical",
  affected_component }
```

Markdown is opaque — unqueryable, unindexable, and impossible to validate. Structured output enables `GROUP BY probable_cause`, severity filtering, and a defined failure mode: invalid JSON triggers one repair-prompt retry, then the DLQ.

### 5.2 Resilience

| Concern | Mechanism |
|---|---|
| Provider outage | Circuit breaker (`opossum`): opens after 5 consecutive failures, half-open at 30s |
| Slow response | `AbortController`, 30s timeout |
| Transient failure | Exponential backoff with jitter, max 3 attempts |
| Runaway cost | Redis counter `llm:budget:{YYYY-MM-DD}`, `INCRBY` estimated tokens; over cap → DLQ with `reason: budget_exceeded` |
| Terminal failure | Produce to `dead-letter`, release the claim, commit offset |

## 6. Persistence

```sql
CREATE TABLE incidents (
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
CREATE INDEX ON incidents (service_id, created_at DESC);
CREATE INDEX ON incidents (fingerprint, created_at DESC);

CREATE TABLE dead_letters (
  id uuid PRIMARY KEY, source_topic text, payload jsonb,
  error text, attempts int, created_at timestamptz DEFAULT now()
);
```

Driver: `pg` with hand-written SQL. No ORM — an ORM is an abstraction that would have to be defended rather than explained.

Hosting: Postgres in Docker Compose for development; managed Postgres (Railway or Neon) at deploy. A connection-string swap.

## 7. Dashboard

Fastify + `@fastify/websocket`, consuming `diagnosed-incidents` with a **unique consumer group per instance**: `dashboard-${hostname}-${pid}`.

Consumer groups split work — two dashboards sharing a group would each receive roughly half the incidents. Fan-out requires a distinct group per instance. Same topic, opposite semantics, one config field apart.

On connect: backfill the last 50 incidents from Postgres, then attach the live stream.

## 8. Failure handling

1. **Manual offset commits.** `autoCommit: false`; commit only after full processing. At-least-once delivery combined with the Redis claim yields effectively-once LLM invocation.
2. **Graceful shutdown.** SIGTERM → stop fetching → drain in-flight → flush producer → close Redis/PG → exit; hard 10s timeout then force. Implemented once in `shared/kafka.js`.
3. **Poison-pill isolation.** Every handler body wrapped in try/catch; parse failures go directly to the DLQ. A throw inside `eachMessage` is retried indefinitely by KafkaJS and stalls the entire partition — one malformed line would halt every service sharing it.

## 9. Observability

`prom-client` → Prometheus → Grafana, all in Compose, dashboard JSON committed to the repo.

Metrics: ingest rate and latency histogram; consumed / deduped / suppressed counters; incidents created; LLM outcome, latency and token counters; DLQ by reason; circuit-breaker state; and **consumer lag per partition**, computed on a 10s interval by an admin client diffing committed offsets against topic end offsets.

Consumer lag is the primary health signal of any streaming system — the difference between "it works" and "it is keeping up."

## 10. Testing

- **Unit** — fingerprint normalization (table-driven), envelope validation.
- **Integration** — Testcontainers running real Redpanda, Redis and Postgres; produce 1,000 logs across 3 services, assert the exact expected incident count.
- **Chaos** — under sustained load, `docker kill` a filter worker; assert `messages_in == messages_processed` after recovery.
- **Load** — k6 against the ingest endpoint.

## 11. Build order

| Week | Work | Milestone |
|---|---|---|
| 1 | Compose up. Shared modules. Ingest → produce. Filter worker consumes and logs. | A POSTed log line appears in the worker |
| 2 | Fingerprinting, Lua window, claiming. AI worker against a **fake LLM**. Postgres write. | A burst of 60 errors produces one incident row |
| 3 | Real Groq call, structured output, circuit breaker, retries, budget cap, DLQ. Dashboard + WebSocket + backfill. | Full end-to-end demo |
| 4 | Manual offsets, graceful shutdown, poison-pill handling. Prometheus + Grafana. Testcontainers + chaos test. | `docker kill` mid-load with zero loss |
| 5 | Producer batching, k6 load test, before/after numbers. README, architecture diagram, roadmap. Deploy. | Public link + demo GIF |

Week 2's fake LLM is a deliberate seam: the AI worker depends on an `analyze(context) → Incident` interface, not on Groq. Weeks 1–2 need no API key and no network, and week 4's tests run in CI deterministically without spending tokens.

Week 5's batching is deliberately deferred so that both the naive and optimized throughput numbers exist. The measured delta and the `acks` durability trade-off are worth more than a system that was fast from the start with no story attached.

## 12. Learning and interview-prep requirements

Every implementation step must also produce:

1. A short explanation of why the approach was chosen and what was rejected.
2. A syntax primer for the tool touched — the commonly used commands only.
3. Likely interviewer questions with model answers, appended sequentially to `D:\Interview_material_NR\Claude Project QnA.txt`.

The author is learning Kafka, Redis, Docker and Node concurrently with building. Code that works but cannot be explained fails the project's actual objective.
