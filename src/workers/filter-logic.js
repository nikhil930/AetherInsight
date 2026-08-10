import { fingerprint as computeFingerprint } from '../shared/fingerprint.js';
import { recordOccurrence, claimAnalysis, pushContext, readContext } from '../shared/redis.js';

// Order is deliberate: every occurrence is counted BEFORE any deduplication.
// Deduplicating first would collapse an error storm into a single occurrence
// and the threshold would never be crossed — suppressing exactly the incident
// we exist to detect. The claim guards the expensive analysis, not the count.
export async function processLog(deps, log) {
  const { redis, thresholds } = deps;
  const { alertThreshold, windowSeconds, claimTtlSeconds, contextSamples } = thresholds;

  const fingerprint = computeFingerprint(log.message);

  await pushContext(redis, fingerprint, log.message, contextSamples, windowSeconds * 6);

  const count = await recordOccurrence(redis, {
    serviceId: log.service_id,
    fingerprint,
    windowSeconds,
  });

  if (count < alertThreshold) {
    return { action: 'counted', fingerprint, count };
  }

  const won = await claimAnalysis(redis, fingerprint, claimTtlSeconds);
  if (!won) {
    return { action: 'suppressed', fingerprint, count };
  }

  const sampleLogs = await readContext(redis, fingerprint);
  return {
    action: 'escalated',
    fingerprint,
    count,
    request: {
      service_id: log.service_id,
      fingerprint,
      occurrence_count: count,
      window_seconds: windowSeconds,
      sample_logs: sampleLogs,
    },
  };
}
