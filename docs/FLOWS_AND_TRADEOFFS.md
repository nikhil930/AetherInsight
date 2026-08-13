# AetherInsight — Flows & Trade-offs Reference

> Revision-ready reference. Every choice below has a reason. Interviewers
> probe reasons, not code. Read section by section — each is standalone.

Last updated: 2026-08-13 (added §12.18 flat vs JSONB for the incidents table).

---

## 1. Where state lives (the five locations)

| # | Location | Purpose | Data Structure | Key format | Lifespan |
|---|---|---|---|---|---|
| 1 | Kafka `raw-logs` | Firehose of every incoming log, ordered per service | Log-structured commit log | Partition by `service_id` | Retention (e.g. 24h) |
| 2 | Redis `win:<svc>:<fp>` | "Am I over threshold *right now*?" | Sorted Set (ZSET) | Timestamps as scores | 5-min sliding window |
| 3 | Redis `claim:<fp>` | "Has someone already escalated this?" | String with NX + EX | Value = worker id | 15 min (900s TTL) |
| 4 | Redis `ctx:<fp>` | Last 20 raw samples to feed the LLM | List (LPUSH + LTRIM) | JSON strings | 1 h idle TTL |
| 5 | Postgres `incidents` + `analyses` | Durable, queryable source of truth | Relational rows | UUID PKs | Forever |

**Rule of thumb:**
- **Kafka** = ordered event stream (replay + audit).
- **Redis** = *hot* state, tolerable if lost (rebuilds from Kafka replay).
- **Postgres** = *cold* state, must survive a crash.

---

## 2. Filter worker — state machine

For every message consumed from `raw-logs`, the worker produces exactly one of four outcomes:

```
                message arrives
                       │
              ┌────────┴────────┐
        decode ok?          decode fails
              │                  │
              ▼                  ▼
   fingerprint + count       DROPPED
              │                  │
      count < threshold?         │
      ┌───────┴───────┐          │
     yes             no          │
      │               │          │
      ▼               ▼          │
   COUNTED     try SET NX EX     │
              on claim:<fp>      │
              ┌────┴────┐        │
           won      lost         │
              │         │        │
              ▼         ▼        │
         ESCALATED  SUPPRESSED  DROPPED
```

### The four outcomes

| Outcome | Meaning | Side effects |
|---|---|---|
| **COUNTED** | Below threshold — just record it | ZADD to sliding window; LPUSH to context buffer |
| **ESCALATED** | Threshold crossed AND we won the claim | INSERT incident to Postgres; produce to `ai-analysis-requests` |
| **SUPPRESSED** | Threshold crossed BUT claim already held | UPDATE incident.event_count++; do NOT re-call LLM |
| **DROPPED** | Undecodable poison pill | Log + (Task 10) forward to `dead-letter` topic; NEVER rethrow |

**Why never rethrow on poison pills?** A thrown error inside KafkaJS's `eachMessage` retries the same offset forever. That stalls the entire partition — every other service sharing that partition stops processing.

---

## 3. Redis context buffer (`ctx:<fp>`) — what feeds the LLM

**Purpose:** Give the LLM 20 sample raw logs of the same fingerprint so it can spot patterns beyond the single message.

**Ops used:**
```
LPUSH ctx:<fp> <json-sample>    -- prepend, O(1)
LTRIM ctx:<fp> 0 19             -- keep only indexes 0..19
EXPIRE ctx:<fp> 3600            -- reset TTL on every write
LRANGE ctx:<fp> 0 -1            -- read all for LLM prompt
```

### Why a List, not X?

| Alternative | Rejected because |
|---|---|
| **ZSET (sorted set)** | Overkill — we don't need scoring or range queries, just "last 20" |
| **Stream (XADD)** | Newer, richer, but heavier metadata per entry; we don't need consumer groups here |
| **Postgres table** | Too slow, wastes cold storage on hot ephemeral data |
| **In-process array** | Not shared across worker replicas; lost on restart |

### Storage math
20 samples × ~500 B ≈ **10 KB per fingerprint**.
10,000 active fingerprints ≈ **100 MB Redis** — trivial.

---

## 4. TTL — how Redis actually expires keys

Two mechanisms working together:

1. **Lazy expiration** — on every `GET`/`LRANGE`/etc., Redis checks the key's TTL. If expired, delete and return nil.
2. **Active expiration** — every 100 ms, Redis samples ~20 random keys that have a TTL and evicts the expired ones.

**Consequence:** a key with `EXPIRE 3600` doesn't vanish at exactly 3600 s. It vanishes the *next time it's touched* OR when the sampler happens to see it. Memory-wise this is fine because sampling keeps expired-key bloat under 25% by default (`maxmemory-samples` config).

**When it matters:** never assume a key is gone the millisecond its TTL expires. If you need "gone at exactly T", explicitly `DEL` at T.

---

## 5. Sliding window (`win:<svc>:<fp>`) — the count

**Purpose:** count how many events of this fingerprint arrived in the last 5 minutes, per service.

**Ops used (wrap in a single Lua script for atomicity):**
```
ZADD win:<svc>:<fp> <now_ms> <uuid>          -- add this event
ZREMRANGEBYSCORE win:<svc>:<fp> 0 <now-5m>   -- evict old
ZCARD win:<svc>:<fp>                          -- how many remain
EXPIRE win:<svc>:<fp> 300                    -- self-clean if idle
```

### Why ZSET, not INCR + EXPIRE?

A simple `INCR count:<fp>` counter with a 5-min TTL is *tumbling*, not *sliding*: it resets to zero at minute boundaries. You'd miss a storm that spans a boundary.

ZSET gives you a **true sliding window**: at any instant, "count since t-5m" is exact.

### Why atomic Lua script?

Without Lua, three round trips between worker and Redis leave a race window: two workers could both see `count == 49`, both increment to 50, both try to escalate. Lua runs server-side as one atomic operation — one worker gets `count = 50` and escalates, the other gets `count = 51`.

---

## 6. Idempotency claim (`claim:<fp>`) — one incident per storm

**Purpose:** when the threshold is crossed, only ONE worker should call the (expensive, slow) LLM. Everyone else who also crosses the threshold for the same fingerprint should shut up.

**The one command:**
```
SET claim:<fp> <worker_id> NX EX 900
```

| Flag | Meaning | Why |
|---|---|---|
| `NX` | Only set if key does not exist | Atomic check-and-set — the winner is decided by Redis, not by the workers |
| `EX 900` | Auto-expire after 900 s | If the winning worker crashes mid-analysis, the claim releases itself — no orphan locks |

- `OK` return → **I own it** → escalate to LLM
- `nil` return → **someone else owns it** → suppress

### Why 15 min (900 s)?

Trade-off table:

| Duration | Risk |
|---|---|
| 60 s (too short) | LLM re-invoked on the same ongoing storm every minute — expensive and noisy |
| 900 s (chosen) | Sensible for portfolio; matches typical incident triage window |
| 1 h (too long) | A *new* problem with the same fingerprint (fresh outage after recovery) gets silently swallowed |

**Tunable via env var** later; not a fixed law.

### Why not use a distributed lock library (Redlock)?

- Kafka partition ownership already gives us **single-writer-per-service**. Two workers *cannot* both be processing the same service_id at the same time.
- Redlock adds a dependency, retry logic, and clock-skew failure modes.
- `SET NX EX` is one command, does what we need, is understood by everyone reading the code.

Removed from original design; call this out in interviews as a **scope-reduction decision**.

---

## 7. LLM payload — cost, latency, optimizations

### Baseline numbers
| Metric | Value |
|---|---|
| Samples sent | 20 |
| Bytes per sample | ~500 B |
| Total payload | ~10 KB |
| Approx. tokens (input) | ~2,000 |
| Approx. cost (Sonnet) | ~$0.006 per analysis |
| Approx. latency | ~2–3 s |

### Scale optimizations (know these for interviews)

| Technique | What it does | Real-world example |
|---|---|---|
| **Structural dedup** | If 20 samples all match the same template, send 3 + "seen 47 times" | Sentry issue grouping |
| **Sampling** | Store 1-in-N raw logs during storms | Datadog, Honeycomb |
| **MessagePack** | Binary encoding — 30–50% smaller than JSON | Kafka payloads, Redis modules |
| **Reservoir sampling** | Keep a *representative* 20, not just the *last* 20 | Cloudflare Radar |
| **Prompt caching** | Anthropic caches system prompt at 90% discount | Claude API |

**AetherInsight v1 does the simple thing** (last-20, no sampling). *Knowing* these techniques matters more than *implementing* them for a portfolio project.

---

## 8. Postgres — the durable source of truth

### Tables

```sql
incidents (
  id             uuid PK,
  fingerprint    text,
  service_id     text,
  first_seen_at  timestamptz,
  last_seen_at   timestamptz,
  event_count    int,
  status         text,      -- 'open' | 'analyzing' | 'resolved'
  severity       text,      -- LLM-classified
  sample_message text,
  created_at     timestamptz
);

analyses (
  id            uuid PK,
  incident_id   uuid FK → incidents,
  model         text,       -- 'claude-sonnet-4', 'fake', etc.
  root_cause    text,
  suggested_fix text,
  confidence    numeric,
  prompt_tokens int,
  latency_ms    int,
  created_at    timestamptz
);
```

### Why two tables, not one?

**One incident → many analyses.** A better model comes out; on-call disputes the first RCA; we re-analyze with more context. Flattening loses history.

### What is NOT in Postgres, and why

| Data | Where instead | Why not Postgres |
|---|---|---|
| Raw logs | Kafka `raw-logs` | Millions of rows/day; Kafka is cheaper and replayable |
| Live counters | Redis `win:<svc>:<fp>` | Postgres can't do 100K writes/s cheaply |
| LLM prompt payloads | Reconstruct from Kafka + context buffer | Storage bloat with no query benefit |
| Suppressed dupes | Rolled into `incidents.event_count` | One row per storm, not per event |

**Design principle:** Postgres holds *summarized state*, not the firehose.

---

## 9. Count during suppression — the subtle part

**Two counters exist. They do different jobs.** Confusing them is the #1 mistake.

| Counter | Where | Question it answers | Lifespan |
|---|---|---|---|
| `ZCARD win:<svc>:<fp>` | Redis | "Am I over threshold *right now*?" | Rolling 5 min |
| `incidents.event_count` | Postgres | "How many events belonged to this incident total?" | Forever |

### What happens during the 15-min suppression window

```
Event #50 arrives → threshold hit → claim WON  → INSERT incidents(count=50)
Event #51 arrives → threshold hit → claim LOST → UPDATE incidents SET event_count = event_count + 1, last_seen_at = now()
Event #52 arrives → threshold hit → claim LOST → UPDATE event_count = event_count + 1
...
Event #4732 arrives → same UPDATE
```

**Without the UPDATE,** the on-call engineer would open the incident 10 min later and see "event_count: 50" and think the storm ended. With it, they see "event_count: 4732, still active" — which is the truth.

### Simpler alternative (mention as a trade-off)

Instead of UPDATE-per-event during suppression:
- Skip the writes during the window
- When the claim expires at 900 s, do one `ZCARD` and one final UPDATE

**Trade-off:** loses real-time `last_seen_at`, but removes thousands of Postgres writes. Fine for v1; worth calling out.

---

## 10. Master trade-off table (one-shot revision)

| Decision | Chosen | Alternative rejected | Why chosen |
|---|---|---|---|
| Broker | Redpanda | Apache Kafka | No JVM/ZK, single binary, ~2s startup |
| Partition key | `service_id` | Random / round-robin | Single-writer-per-service = no distributed lock |
| Dedup ordering | Count BEFORE dedup | Dedup BEFORE count | Dedup-first would suppress the storms we exist to detect |
| Mutex | `SET NX EX` | Redlock | Partition ownership already gives us mutex; Redlock is redundant |
| Sliding window | ZSET + Lua | INCR + EXPIRE | INCR is tumbling, not sliding — misses boundary-spanning storms |
| Context buffer | Redis List + LTRIM | ZSET / Stream / DB | List is O(1) prepend, no metadata bloat, LTRIM caps cost |
| Fingerprint hash | sha1 | md5 / sha256 | md5 deprecated by convention; sha256 wastes CPU + key bytes |
| DB | Postgres | Supabase | Supabase's realtime would replace WebSocket layer we're building |
| DB access | Hand-written SQL | Prisma / Drizzle | Portfolio value is in *understanding* queries, not ORM syntax |
| Config validation | zod `parse` (crash) | zod `safeParse` | Config must be right before startup; crash loud > run misconfigured |
| Payload validation | zod `safeParse` (400) | `parse` (crash) | One bad request can't kill the whole server |
| HTTP status on /ingest | 202 Accepted | 200/201 | Work isn't done — it's queued. 202 is the correct semantic |
| API dep injection | `createApp({ producer })` | Singleton import | Tests pass a fake; no NODE_ENV branching |
| Envelope | `{v, type, id, ts, payload}` on every message | Bare payload | `v` = schema evolution; `id` = idempotency; `type` = dispatch |
| Poison pill handling | Catch + log + drop | Rethrow | Rethrow stalls the partition forever |

---

## 11. Interview-style probe map

If asked...

- **"Walk me through what happens when a log comes in"** → Section 2 + Section 9
- **"How do you prevent duplicate LLM calls?"** → Section 6
- **"Why Redis, not just Postgres?"** → Section 1 (rule of thumb)
- **"What's your consistency model?"** → Kafka = at-least-once, filter dedup via claim = effectively-once for LLM calls, Postgres UPDATE is idempotent-safe
- **"How would you scale this to 100× traffic?"** → Section 7 optimizations + more Kafka partitions + Redis Cluster + Postgres read replicas
- **"What happens if Redis crashes?"** → Sliding windows lost → some duplicate LLM calls during recovery → Postgres still consistent because `incident_id` UUIDs are generated by workers and the AI worker can dedup on insert
- **"What happens if a worker crashes mid-analysis?"** → Claim TTL (900s) expires → next threshold hit re-claims → LLM re-invoked on the same storm (acceptable trade for simplicity)
- **"Why not use a queue like SQS instead of Kafka?"** → SQS is per-message; no ordering, no replay, no partition-as-mutex. Kafka's log-structured design *is* the feature.

---

## 12. Technology choices — long (learning) + short (interview)

> For every choice: **Interview** is what you *say* (crisp, one breath). **Long** is what you *know* (why, alternatives, trade-offs). Read Long once, rehearse Interview until it flows.

---

### 12.1 Redpanda vs Apache Kafka

**Interview (10 s):**
> "Redpanda is Kafka-wire-compatible but written in C++ with no JVM and no ZooKeeper. For a local portfolio it boots in ~2 s vs Kafka's 30 s, uses ~1/6 the RAM, and every client library still works because the protocol is identical."

**Long (learning):**
- **Kafka's baggage:** JVM (500 MB+ heap), ZooKeeper (extra service, extra config), 20–30 s cold start. Fine in prod, painful for `docker compose up` in dev.
- **Redpanda's design:** single C++ binary, thread-per-core (Seastar framework, same as ScyllaDB), Raft consensus baked in (no ZK), no page cache (direct I/O).
- **Wire compatibility:** implements the Kafka protocol — KafkaJS/librdkafka/Java clients don't know the difference. Zero-cost migration in either direction.
- **What Kafka does better:** more mature ecosystem (Kafka Connect, Streams, MirrorMaker), larger community, more battle-tested at extreme scale (LinkedIn runs it).
- **When to pick Kafka anyway:** you already have JVM ops muscle, you need Kafka Streams DSL, or Confluent Cloud is your compliance-approved vendor.

---

### 12.2 Node.js vs Go / Python / Java

**Interview (10 s):**
> "Node fits an I/O-bound pipeline — most time is spent waiting on Kafka, Redis, Postgres, and the LLM API. Its event loop handles thousands of concurrent connections without threads, and one language across API + workers + dashboard reduces context switching."

**Long (learning):**
- **I/O vs CPU:** AetherInsight barely computes anything (regex, hashing). The bottleneck is network. Node's single-threaded event loop is designed for exactly this.
- **Go:** faster, statically typed, better for CPU-heavy work. Would win for a system doing analytics inside the worker. Adds a build step and a language boundary between backend and dashboard.
- **Python:** great for data science but the GIL hurts concurrent I/O; async story (asyncio) is younger and less consistent than Node's.
- **Java:** production-grade, but JVM warmup + verbosity slow iteration. Better if the team is already Java-shaped.
- **Real trade-off admitted:** Node lacks true parallelism for CPU work. If the fingerprinting step ever became a bottleneck we'd offload it to a worker thread or a native binding.

---

### 12.3 Redis vs Memcached vs in-process cache

**Interview (10 s):**
> "Redis has the data structures we need — ZSETs for sliding windows, Lists for context buffers, atomic SET NX EX for claims. Memcached is only strings, and in-process caches can't be shared across worker replicas."

**Long (learning):**
- **Memcached:** simpler, slightly faster for pure key/value, but no data structures, no persistence, no pub/sub, no Lua. We'd have to marshal every ZSET operation to a string blob — losing atomicity.
- **In-process (Node Map):** zero network hop, but state dies with the process and doesn't sync across replicas. A restart during a storm loses all counts.
- **Redis Streams as alternative to Kafka:** tempting for small setups. Rejected because Kafka's partition-as-mutex property is core to our design (see Section 6).
- **Persistence trade-off:** we run Redis without AOF (append-only file) in dev; if it crashes we lose the hot state — but Kafka replay rebuilds it. In prod you'd enable RDB snapshots every 5 min as a safety net.

---

### 12.4 Postgres vs MySQL vs MongoDB

**Interview (10 s):**
> "Postgres gives us JSONB for flexible payloads *and* strong relational integrity for the incidents → analyses one-to-many. MySQL's JSON is weaker, and MongoDB throws away joins that we actually want."

**Long (learning):**
- **Relational fit:** an incident has many analyses (re-run with a better model, on-call dispute, historical audit). Foreign keys and joins are the natural expression.
- **JSONB advantage:** the `analyses.suggested_fix` field can hold arbitrary LLM output structure that evolves without a schema migration. MySQL supports JSON but with weaker indexing and no GIN.
- **MongoDB:** would flatten the design, but you'd rebuild joins in application code and lose ACID across incidents+analyses.
- **Why not Supabase (Postgres-as-a-service):** Supabase bundles realtime subscriptions that would duplicate the WebSocket layer we're building for learning. Also adds vendor coupling.
- **Extensions we might use later:** `pg_partman` (partition incidents by month), `pgvector` (embed analyses for similarity search).

---

### 12.5 Fastify vs Express

**Interview (10 s):**
> "Fastify is ~2× Express's throughput, has built-in schema validation, and returns proper HTTP semantics by default. Express is older and more familiar but requires layering on validators, loggers, and TypeScript types."

**Long (learning):**
- **Perf:** Fastify's radix-tree router + JSON schema serialization avoid Express's per-request middleware chain overhead.
- **Validation:** first-class JSON schema (which pairs with zod via ecosystem plugins). Express needs `express-validator`, `joi`, or hand-rolled checks.
- **Modern defaults:** async/await native, plugin encapsulation, request-scoped logging. Express predates these idioms.
- **When Express wins:** if you inherit a huge Express codebase or need obscure middleware only that ecosystem has.

---

### 12.6 ioredis vs node-redis

**Interview (10 s):**
> "ioredis has first-class Cluster/Sentinel support, auto-pipelining, and native Lua defineCommand. node-redis v4 caught up on features but ioredis remains the safer choice for anything Cluster-shaped."

**Long (learning):**
- **defineCommand:** ioredis registers a Lua script once with SCRIPT LOAD, then calls it by SHA. node-redis needs manual scripting boilerplate.
- **Auto-pipelining:** ioredis batches commands issued in the same event-loop tick — free throughput.
- **Cluster + Sentinel:** ioredis was built for these; node-redis added them later.
- **Trade-off:** ioredis is heavier; node-redis is officially maintained by Redis Ltd. now and has cleaner promise API. Both are fine for single-node use.

---

### 12.7 KafkaJS vs node-rdkafka

**Interview (10 s):**
> "KafkaJS is pure JS — no native binding to compile, works on Windows out of the box, and has clean promise-based APIs. node-rdkafka wraps the C++ librdkafka, giving higher throughput but painful install."

**Long (learning):**
- **Install pain:** node-rdkafka needs a C++ toolchain and matching OpenSSL. KafkaJS is `npm install` and done.
- **Throughput:** node-rdkafka can push ~10× more messages/sec at extreme scale. Irrelevant for a portfolio project measured in dozens of messages/sec.
- **Features:** KafkaJS is missing some newer Kafka APIs (e.g., transactions maturity, header-based interceptors). Fine for our use.
- **Cross-platform:** matters here — you're on Windows.

---

### 12.8 Lua script vs MULTI/EXEC (Redis atomicity)

**Interview (10 s):**
> "Lua runs the whole logic server-side in one round trip and can branch on values Redis returns. MULTI queues commands but can't make decisions between them — you'd need optimistic locking with WATCH, which retries under contention."

**Long (learning):**
- **MULTI/EXEC:** batches commands into one atomic block, but every command is queued client-side and dispatched together. You cannot use the result of command N to decide command N+1 — that requires a round trip back to the client, breaking atomicity.
- **WATCH (optimistic locking):** MULTI + WATCH detects concurrent mutation and aborts, expecting client retry. Under high contention (storm scenario) this retries in a hot loop.
- **Lua:** Redis is single-threaded, so a Lua script runs to completion with nothing else touching the keys. You get **atomicity + branching + one round trip**.
- **Cost:** Lua blocks the whole Redis instance for its duration. Keep scripts short (< 1 ms). Ours does 4 ops → well within budget.
- **Loading:** ioredis's `defineCommand` calls `SCRIPT LOAD` once; subsequent calls send the SHA + args, not the whole script — cheap.

**Alternative rejected:** Redis Functions (7.0+) — same benefit, requires explicit registration + slightly worse client support. Lua is the pragmatic choice.

---

### 12.9 Hand-written fingerprint rules vs Drain3

**Interview (10 s):**
> "Drain3 learns templates online from a live log stream — powerful but non-deterministic across restarts and an extra service to run. For a bounded set of known log shapes, hand-rules are deterministic, testable, and reviewable."

**Long (learning):**
- **Drain (paper, 2017):** builds a parse tree that groups messages by first token, then depth-N tokens, refining templates as new logs arrive. Adapts to formats you didn't anticipate.
- **Why powerful:** on a real production stream with dozens of services and thousands of log shapes, Drain discovers what regex authors miss.
- **Why we skip it:** (a) needs a running service or in-process state that survives restarts; (b) template tree isn't reproducible unless seeded the same way; (c) an interviewer asking "how did that template get chosen?" gets a shrug.
- **Escape hatch:** in Section 11's answer to "how would you scale to novel formats?" — Drain3 is the correct upgrade path.

**Related:** commercial tools (Splunk, Datadog, Sumo Logic) run more sophisticated variants — often ML-based clustering over embeddings. Same trade-off: automation vs explainability.

---

### 12.10 sha1 vs md5 vs sha256 (for fingerprint hash)

**Interview (10 s):**
> "None of the crypto properties matter — we're not defending against attackers. sha1 gives us 40-char keys, uniform distribution, and negligible collision risk at our scale (~10⁵ distinct fingerprints ever). md5 is deprecated by convention; sha256 wastes CPU and key bytes."

**Long (learning):**
- **md5:** functionally fine, but its presence in code invites "why not sha256?" questions in reviews. Cheap political tax to avoid.
- **sha256:** 2× CPU cost, 64-char hex output. Every Redis key is 24 more bytes; every log line 24 more bytes; scales negatively.
- **Birthday math for sha1:** collision at 50% probability needs ~2⁸⁰ items. At 1% probability, ~10²². Real deployments see 10⁵ fingerprints. Collision is a rounding error.
- **When you'd upgrade to sha256:** anything user-facing where a collision would let a user impersonate another, or content-addressed storage where a hash is a security boundary. Neither applies here.

---

### 12.11 Raw SQL (pg) vs ORM (Prisma / Drizzle / TypeORM)

**Interview (10 s):**
> "Portfolio value is in *understanding* what queries hit the database. ORMs add a translation layer you'd have to explain anyway, plus migrations and a schema DSL. `pg` gives us direct SQL — fewer dependencies, no query surprises."

**Long (learning):**
- **Prisma:** great DX for CRUD, but N+1 queries hide behind syntax, and complex queries fall back to `$queryRaw` anyway. Interviewer asks "show me the actual SQL you run" and you're squinting at generated queries.
- **Drizzle:** thin, closer to raw SQL, type-safe. Legitimate choice for a larger project. Still an abstraction to explain.
- **TypeORM:** decorator-heavy, JVM-inspired, weaker types than Drizzle.
- **What we lose without an ORM:** migration tooling (we'd use `node-pg-migrate` or write plain `.sql` files), automatic schema types (we hand-write them), relation loading helpers.
- **What we gain:** every query is visible; every performance issue is inspectable; nothing hides behind `include: { analyses: true }`.

---

### 12.12 zod vs joi / yup / ajv

**Interview (10 s):**
> "zod infers TypeScript types from the schema — one source of truth for runtime + compile-time. Joi is powerful but JS-first with weak type inference; ajv is the fastest but you write JSON schema by hand."

**Long (learning):**
- **Type inference:** `z.infer<typeof Schema>` gives you a TS type for free. Joi requires separate interface declarations that drift.
- **API ergonomics:** zod's chainable API reads like TS. Joi's `Joi.object({...}).keys(...)` feels older.
- **Performance:** ajv (JSON Schema compiler) is 5–10× faster because it precompiles. Matters at millions-of-validations/sec; irrelevant at our scale.
- **Ecosystem:** zod pairs cleanly with tRPC, Astro, Next.js server actions — the "modern TS stack" default.

---

### 12.13 Vitest vs Jest

**Interview (10 s):**
> "Vitest uses Vite's transform pipeline — native ESM, TypeScript out of the box, ~3× faster startup than Jest. Jest is more mature but ESM support has been rocky and requires Babel or ts-jest layers."

**Long (learning):**
- **ESM native:** we use `import`/`export` and top-level await. Jest needed experimental flags; Vitest just runs.
- **Speed:** Vitest reuses Vite's dep-optimization cache. Cold start is fast, warm reruns are instant.
- **Compatible API:** `describe/it/expect` from Jest works verbatim — migration cost is near zero.
- **Watch mode:** Vitest re-runs only affected tests via the same dep graph Vite uses for HMR.

---

### 12.14 Docker Compose vs Kubernetes (local dev)

**Interview (10 s):**
> "Compose is the right primitive for local dev — one YAML, one `up` command, direct volume mounts. Kubernetes is for production orchestration and adds massive local overhead (kind/minikube, kubelets, manifests) without a benefit at dev scale."

**Long (learning):**
- **Compose scope:** define containers, networks, volumes, healthchecks. Perfect for "Redis + Kafka + Postgres + workers, all wired together, one command."
- **k8s value:** rolling updates, autoscaling, self-healing, service mesh integration. All meaningless on a laptop.
- **Migration path:** the `docker-compose.yml` maps cleanly onto Helm charts / Kustomize when you're ready to deploy. The images don't change; only the orchestrator does.

---

### 12.15 MessagePack vs JSON (payload encoding, future optimization)

**Interview (10 s):**
> "JSON is human-readable and universal — right default. MessagePack shaves 30–50% off payload size for numbers-heavy data, cheap CPU, but every consumer needs the decoder. Worth it for Kafka message headers or LLM prompts at scale; not for dev-time debuggability."

**Long (learning):**
- **How msgpack saves bytes:** small ints as 1 byte, no field-name repetition per record (with schema), compact representation for arrays.
- **Where it hurts:** you can't `cat` a message and read it. Every polyglot consumer needs a decoder. Kafka UI tools show binary.
- **Compression as alternative:** Kafka supports zstd/lz4 compression per topic — often better savings than msgpack, transparent to consumers.
- **AetherInsight v1 uses JSON everywhere** and enables Kafka zstd compression later if payload size becomes a bottleneck.

---

### 12.16 Claude (Anthropic) vs OpenAI vs Ollama (local)

**Interview (10 s):**
> "Claude Sonnet is strong at code and structured output, has 200K context, and prompt caching gives 90% discount on repeated system prompts — good fit for RCA analysis. OpenAI is a fine alternative; Ollama would give us free local inference for dev but slower and less capable."

**Long (learning):**
- **Sonnet fit:** RCA output benefits from long context (paste in 20 samples + service metadata + past incidents) and reliable JSON structure.
- **Prompt caching:** Anthropic caches your system prompt for 5 min at 90% off. Our workers hit the same prompt every analysis — massive savings.
- **OpenAI trade-off:** function calling is more mature; latency is comparable; pricing similar tier.
- **Ollama (local):** LLaMA 3.1 8B runs on a decent laptop, free per-request, no API dependency. Slower, weaker at structured output, but perfect for offline dev and demos.
- **Design decision:** analyzer is an *interface* (Task 11) — swap Anthropic/OpenAI/Ollama/fake by config. Never hard-code a provider.

---

### 12.17 WebSocket vs SSE (future: dashboard live updates)

**Interview (10 s):**
> "SSE is server-to-client only, uses plain HTTP, auto-reconnects, and works through corporate proxies. WebSockets are bidirectional and higher throughput. For a dashboard where users read but never send, SSE is simpler and enough."

**Long (learning):**
- **SSE simplicity:** one HTTP GET, keeps connection open, server writes `data: {...}\n\n` events. No handshake protocol, no ping/pong logic.
- **WebSocket wins:** two-way (server can send + client can send), binary frames, lower per-message overhead.
- **Reconnect:** SSE reconnects itself with Last-Event-ID header. WebSocket needs library-level logic.
- **Auth:** SSE cookies + custom headers via EventSource polyfill; WebSocket has query-string tokens or Sec-WebSocket-Protocol tricks.
- **Choice for AetherInsight:** dashboard only shows incident feed → SSE. If we add chat-with-incident later → WebSocket.

---

### 12.18 Flat columns vs JSONB payload (the `incidents` table)

**Interview (10 s):**
> "The incident row's fields are known and small — service_id, fingerprint, summary, cause, fix, severity, counts, LLM metadata. Flat columns give me typed storage, indexable filters (`WHERE severity='high'`), and SQL that reads like the domain. JSONB would push all of that into `payload->>'severity'` casts and a GIN index — right choice when the shape is unknown or wildly variable, wrong choice when it isn't."

**Long (learning):**
- **What flat gives us:**
  - Column types enforce shape (`confidence real`, `occurrence_count int`, `created_at timestamptz`) — malformed writes fail at INSERT, not at read time.
  - Btree indexes on `(service_id, created_at DESC)` and `(fingerprint, created_at DESC)` — the two access patterns the dashboard needs — are cheap and stay tight.
  - `EXPLAIN` shows real column reads, not `jsonb_path_ops` gymnastics.
  - Migrations are visible: `ALTER TABLE incidents ADD COLUMN ...` is a diff a reviewer can approve.
- **What JSONB would have given us:**
  - Zero-migration additions: LLM changes its output shape? Just write the new key.
  - Native document semantics; the whole LLM response goes in as one blob.
  - GIN indexes for arbitrary key lookup (`payload @> '{"severity":"high"}'`) — powerful but pricey.
- **Why JSONB is the wrong pick here specifically:**
  - The incident shape is *known* — the analyzer schema (Task 11) will enforce it before Postgres ever sees the row. There is no "unknown shape" problem to solve.
  - Every dashboard query is either "recent incidents for service X" or "distinct fingerprints in last hour" — both are relational filter-and-sort, not document-shape queries.
  - Interviewer test: "show me the query that lists high-severity incidents from payments in the last hour." Flat: `SELECT id, title, created_at FROM incidents WHERE service_id='payments' AND severity='high' AND created_at > now() - interval '1 hour' ORDER BY created_at DESC LIMIT 50` — reads like the sentence. JSONB: `WHERE payload->>'service_id'='payments' AND payload->>'severity'='high'` with `::text` casts everywhere.
- **Where we still use JSONB (compromise):**
  - `sample_logs jsonb` — a variable-length array of raw strings. Storing it as a real column would be `text[]` (Postgres array) or a child table. JSONB is fine because it's read as a whole (fed to the LLM later, never filtered by).
  - `dead_letters.payload jsonb` — by definition an unknown shape; that's what dead-letter storage IS.
- **Migration cost of being wrong:** low. `ALTER TABLE incidents ADD COLUMN raw_payload jsonb` + a UPDATE backfill can convert flat → JSONB later. The reverse (JSONB → flat) is also possible but reveals every place where the LLM wrote an unexpected shape. Start flat; regret is cheap.

---

### 12.19 Master crib sheet (memorize the punchlines)

| Choice | Chosen | One-line why |
|---|---|---|
| Broker | Redpanda | Kafka wire, no JVM/ZK, boots in 2 s |
| Runtime | Node.js | I/O-bound; one language across stack |
| Hot state | Redis | ZSET/List/Lua give us the primitives we need |
| Durable state | Postgres | Relational fit; flat columns for typed writes + indexable filters |
| HTTP framework | Fastify | 2× Express, schema-first, modern defaults |
| Redis client | ioredis | Cluster-ready + defineCommand for Lua |
| Kafka client | KafkaJS | Pure JS, painless install on Windows |
| Atomicity | Lua script | One round trip, branching, single-threaded safety |
| Fingerprint | Hand-regex | Deterministic + testable + reviewable |
| Hash | sha1 | 40-char keys, uniform, negligible collision risk |
| DB access | Raw SQL (`pg`) | Every query visible, no ORM surprises |
| Validation | zod | Type inference = one source of truth |
| Testing | Vitest | ESM-native, ~3× faster startup than Jest |
| Local orch | Docker Compose | Right primitive for dev, k8s is prod-only |
| Wire format | JSON (+ zstd later) | Debuggability first, compress if needed |
| LLM | Claude via interface | Prompt caching + 200K context; swap-by-config |
| Live updates | SSE (planned) | Server-to-client only, plain HTTP, auto-reconnect |
