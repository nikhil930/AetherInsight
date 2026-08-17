import { describe, it, expect, beforeEach, beforeAll, afterAll } from 'vitest';
import { randomUUID } from 'node:crypto';
import { createDb, closeDb, getIncident } from '../src/shared/db.js';
import { createFakeAnalyzer } from '../src/shared/analyzer.js';
import { config } from '../src/shared/config.js';
import { processRequest } from '../src/workers/ai-worker-logic.js';
import { TOPICS } from '../src/shared/kafka.js';
import { decode } from '../src/shared/events.js';

const silentLogger = {
  info:  () => {},
  warn:  () => {},
  error: () => {},
  debug: () => {},
};

function createFakeProducer() {
  const sent = [];
  return {
    sent,
    async send(msg) { sent.push(msg); },
  };
}

function makeEvent({ payload: payloadOverrides = {}, ...envelopeOverrides } = {}) {
  return {
    v:    1,
    type: 'analysis.requested',
    id:   randomUUID(),
    ts:   new Date().toISOString(),
    ...envelopeOverrides,
    payload: {
      service_id:       'payments-api',
      fingerprint:      'a'.repeat(40),
      occurrence_count: 87,
      window_seconds:   10,
      sample_logs: [
        'ECONNREFUSED connecting to postgres at 10.0.1.5:5432',
        'ECONNREFUSED connecting to postgres at 10.0.1.5:5432',
        'ECONNREFUSED connecting to postgres at 10.0.1.5:5432',
      ],
      ...payloadOverrides,
    },
  };
}

describe('processRequest', () => {
  let db;

  beforeAll(() => {
    db = createDb(config.DATABASE_URL);
  });

  afterAll(async () => {
    await closeDb(db);
  });

  beforeEach(async () => {
    await db.query('TRUNCATE incidents, dead_letters');
  });

  it('happy path: analyzes, inserts incident, produces to diagnosed-incidents', async () => {
    const analyzer = createFakeAnalyzer();
    const producer = createFakeProducer();
    const event = makeEvent();

    const result = await processRequest(
      { analyzer, db, producer, logger: silentLogger },
      event,
    );

    expect(result.action).toBe('diagnosed');
    expect(result.incidentId).toBe(event.id);

    const row = await getIncident(db, event.id);
    expect(row).not.toBeNull();
    expect(row.service_id).toBe('payments-api');
    expect(row.fingerprint).toBe('a'.repeat(40));
    expect(row.title).toContain('aaaaaaaa');
    expect(row.severity).toBe('medium'); // 87 occurrences → medium
    expect(row.llm_latency_ms).toBeGreaterThanOrEqual(0);

    expect(producer.sent).toHaveLength(1);
    expect(producer.sent[0].topic).toBe(TOPICS.DIAGNOSED);
    expect(producer.sent[0].messages[0].key).toBe('payments-api');
    const outEvent = decode(producer.sent[0].messages[0].value);
    expect(outEvent.type).toBe('incident.diagnosed');
    expect(outEvent.payload.incident_id).toBe(event.id);
    expect(outEvent.payload.severity).toBe('medium');
  });

  it('idempotent: same event twice → 1 DB row, 1 produced message', async () => {
    const analyzer = createFakeAnalyzer();
    const producer = createFakeProducer();
    const event = makeEvent();
    const deps = { analyzer, db, producer, logger: silentLogger };

    const first  = await processRequest(deps, event);
    const second = await processRequest(deps, event);

    expect(first.action).toBe('diagnosed');
    expect(second.action).toBe('duplicate');

    const { rows } = await db.query('SELECT COUNT(*)::int AS c FROM incidents WHERE id = $1', [event.id]);
    expect(rows[0].c).toBe(1);

    expect(producer.sent).toHaveLength(1);
  });

  it('analyzer failure → dead_letter row, no incident, no produce', async () => {
    const analyzer = createFakeAnalyzer({ failEvery: 1 }); // every call fails
    const producer = createFakeProducer();
    const event = makeEvent();

    const result = await processRequest(
      { analyzer, db, producer, logger: silentLogger },
      event,
    );

    expect(result.action).toBe('failed');
    expect(result.reason).toBe('analyzer');

    const incident = await getIncident(db, event.id);
    expect(incident).toBeNull();

    expect(producer.sent).toHaveLength(0);

    const { rows } = await db.query('SELECT source_topic, error FROM dead_letters');
    expect(rows).toHaveLength(1);
    expect(rows[0].source_topic).toBe(TOPICS.ANALYSIS_REQUESTS);
    expect(rows[0].error).toMatch(/^analyzer:/);
  });

  it('severity ladder: high occurrence_count → severity high', async () => {
    const analyzer = createFakeAnalyzer();
    const producer = createFakeProducer();
    const event = makeEvent({ payload: { occurrence_count: 300 } });

    const result = await processRequest(
      { analyzer, db, producer, logger: silentLogger },
      event,
    );

    expect(result.action).toBe('diagnosed');
    expect(result.incident.severity).toBe('high');

    const row = await getIncident(db, event.id);
    expect(row.severity).toBe('high');
  });

  it('two different events → 2 rows, 2 produced messages, correct linkage', async () => {
    const analyzer = createFakeAnalyzer();
    const producer = createFakeProducer();
    const deps = { analyzer, db, producer, logger: silentLogger };

    const e1 = makeEvent({ payload: { fingerprint: 'a'.repeat(40) } });
    const e2 = makeEvent({ payload: { fingerprint: 'b'.repeat(40) } });

    await processRequest(deps, e1);
    await processRequest(deps, e2);

    const { rows } = await db.query('SELECT id FROM incidents ORDER BY created_at');
    expect(rows).toHaveLength(2);
    expect(new Set(rows.map((r) => r.id))).toEqual(new Set([e1.id, e2.id]));

    expect(producer.sent).toHaveLength(2);
    const ids = producer.sent.map((s) => decode(s.messages[0].value).payload.incident_id);
    expect(new Set(ids)).toEqual(new Set([e1.id, e2.id]));
  });
});
