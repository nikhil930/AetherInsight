import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { randomUUID } from 'node:crypto';
import { createDb, closeDb, insertIncident, getIncident, listIncidents } from '../src/shared/db.js';
import { config } from '../src/shared/config.js';

let pool;

beforeAll(() => {
  pool = createDb(config.DATABASE_URL);
});

afterAll(async () => {
  await closeDb(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE incidents');
});

function makeIncident(overrides = {}) {
  return {
    id: randomUUID(),
    service_id: 'payments',
    fingerprint: 'abc123',
    title: 'DB timeout storm',
    summary: 'Payments service is hitting database timeouts at high rate',
    probable_cause: 'Connection pool exhaustion',
    suggested_fix: 'Increase pool size or investigate slow queries',
    confidence: 0.85,
    severity: 'high',
    occurrence_count: 50,
    window_seconds: 10,
    sample_logs: ['db timeout after 5ms', 'db timeout after 4ms', 'db timeout after 3ms'],
    llm_model: 'claude-sonnet-4',
    llm_tokens: 1200,
    llm_latency_ms: 850,
    ...overrides,
  };
}

describe('db', () => {
  it('inserts an incident and reads it back with all fields intact', async () => {
    const incident = makeIncident();
    const result = await insertIncident(pool, incident);

    expect(result.inserted).toBe(true);
    expect(result.id).toBe(incident.id);

    const row = await getIncident(pool, incident.id);
    expect(row).not.toBeNull();
    expect(row.service_id).toBe('payments');
    expect(row.fingerprint).toBe('abc123');
    expect(row.severity).toBe('high');
    expect(row.occurrence_count).toBe(50);
    expect(row.confidence).toBeCloseTo(0.85, 2);
    expect(row.sample_logs).toEqual(incident.sample_logs);
    expect(row.created_at).toBeInstanceOf(Date);
  });

  it('is idempotent: inserting the same id twice writes exactly one row', async () => {
    const incident = makeIncident();

    const first = await insertIncident(pool, incident);
    expect(first.inserted).toBe(true);

    const second = await insertIncident(pool, { ...incident, title: 'DIFFERENT' });
    expect(second.inserted).toBe(false);

    const row = await getIncident(pool, incident.id);
    expect(row.title).toBe('DB timeout storm');

    const { rows } = await pool.query('SELECT count(*)::int AS c FROM incidents WHERE id = $1', [incident.id]);
    expect(rows[0].c).toBe(1);
  });

  it('returns null when getIncident is called with an unknown id', async () => {
    const row = await getIncident(pool, randomUUID());
    expect(row).toBeNull();
  });

  it('listIncidents returns rows newest-first', async () => {
    const older = makeIncident({ title: 'older' });
    const newer = makeIncident({ title: 'newer' });

    await insertIncident(pool, older);
    await new Promise((r) => setTimeout(r, 20));
    await insertIncident(pool, newer);

    const rows = await listIncidents(pool, { limit: 10 });
    expect(rows).toHaveLength(2);
    expect(rows[0].title).toBe('newer');
    expect(rows[1].title).toBe('older');
  });

  it('listIncidents filters by service_id when provided', async () => {
    await insertIncident(pool, makeIncident({ service_id: 'payments', title: 'p1' }));
    await insertIncident(pool, makeIncident({ service_id: 'orders',   title: 'o1' }));
    await insertIncident(pool, makeIncident({ service_id: 'payments', title: 'p2' }));

    const payments = await listIncidents(pool, { service_id: 'payments' });
    expect(payments).toHaveLength(2);
    expect(payments.every((r) => r.service_id === 'payments')).toBe(true);

    const all = await listIncidents(pool);
    expect(all).toHaveLength(3);
  });

  it('listIncidents pagination respects limit and offset', async () => {
    for (let i = 0; i < 5; i++) {
      await insertIncident(pool, makeIncident({ title: `t${i}` }));
      await new Promise((r) => setTimeout(r, 5));
    }

    const firstPage = await listIncidents(pool, { limit: 2, offset: 0 });
    const secondPage = await listIncidents(pool, { limit: 2, offset: 2 });

    expect(firstPage).toHaveLength(2);
    expect(secondPage).toHaveLength(2);
    expect(firstPage[0].id).not.toBe(secondPage[0].id);
  });
});
