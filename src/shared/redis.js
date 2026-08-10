import Redis from 'ioredis';
import { randomUUID } from 'node:crypto';
import { config } from './config.js';

// The four operations of a sliding window count MUST happen atomically or
// two workers can both read 49, both add one, and both see 50 — producing
// two escalations for one storm. Running them server-side as one Lua script
// is indivisible (Redis is single-threaded) and takes one round trip.
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

export async function recordOccurrence(
  redis,
  { serviceId, fingerprint, windowSeconds, now = Date.now() },
) {
  const key = `win:${serviceId}:${fingerprint}`;
  return redis.slidingWindow(
    key,
    now,
    windowSeconds * 1000,
    randomUUID(),
    windowSeconds * 2,
  );
}

export async function claimAnalysis(redis, fingerprint, ttlSeconds) {
  const result = await redis.set(
    `claim:${fingerprint}`,
    'inflight',
    'NX',
    'EX',
    ttlSeconds,
  );
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
