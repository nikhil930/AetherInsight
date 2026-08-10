import {
  createRedis, recordOccurrence, claimAnalysis, pushContext,
} from '../src/shared/redis.js';

const redis = createRedis();

const args = { serviceId: 'payments', fingerprint: 'abc', windowSeconds: 10 };
await recordOccurrence(redis, args);
await recordOccurrence(redis, args);
await recordOccurrence(redis, args);

await claimAnalysis(redis, 'abc', 900);

for (let i = 1; i <= 4; i++) {
  await pushContext(redis, 'abc', `sample ${i}`, 3, 3600);
}

console.log('seeded');
await redis.quit();
