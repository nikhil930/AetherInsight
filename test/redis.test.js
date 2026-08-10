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
