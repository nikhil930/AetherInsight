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
