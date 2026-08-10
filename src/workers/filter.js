import { runConsumer, createProducer, TOPICS, onShutdown } from '../shared/kafka.js';
import { decode, encode } from '../shared/events.js';
import { createRedis } from '../shared/redis.js';
import { config } from '../shared/config.js';
import { logger } from '../shared/logger.js';
import { processLog } from './filter-logic.js';

const redis = createRedis();
const producer = await createProducer('filter-worker');

const deps = {
  redis,
  thresholds: {
    alertThreshold:   config.ALERT_THRESHOLD,
    windowSeconds:    config.WINDOW_SECONDS,
    claimTtlSeconds:  config.CLAIM_TTL_SECONDS,
    contextSamples:   config.CONTEXT_SAMPLES,
  },
};

const consumer = await runConsumer({
  clientId: 'filter-worker',
  groupId: 'filter-workers',
  topics: [TOPICS.RAW_LOGS],
  handler: async ({ partition, message }) => {
    let event;
    try {
      event = decode(message.value);
    } catch (err) {
      // Poison-pill isolation: never rethrow. A throw here is retried
      // forever on the same offset and stalls the partition for every
      // service sharing it. Task 10 will route these to dead-letter.
      logger.error({ err, partition, offset: message.offset }, 'undecodable message dropped');
      return;
    }

    const result = await processLog(deps, event.payload);

    if (result.action === 'escalated') {
      await producer.send({
        topic: TOPICS.ANALYSIS_REQUESTS,
        messages: [{
          key: result.request.service_id,
          value: encode('analysis.requested', result.request),
        }],
      });
      logger.warn(
        { fingerprint: result.fingerprint, count: result.count, service: result.request.service_id },
        'incident escalated',
      );
    } else {
      logger.debug(
        { action: result.action, count: result.count, fingerprint: result.fingerprint },
        'log processed',
      );
    }
  },
});

onShutdown(async () => {
  await consumer.disconnect();
  await producer.disconnect();
  await redis.quit();
});
