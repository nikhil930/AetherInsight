import { createKafka, TOPICS } from '../src/shared/kafka.js';
import { logger } from '../src/shared/logger.js';

const admin = createKafka('topic-admin').admin();
await admin.connect();

const desired = [
  { topic: TOPICS.RAW_LOGS,          numPartitions: 3, replicationFactor: 1 },
  { topic: TOPICS.ANALYSIS_REQUESTS, numPartitions: 3, replicationFactor: 1 },
  { topic: TOPICS.DIAGNOSED,         numPartitions: 1, replicationFactor: 1 },
  { topic: TOPICS.DEAD_LETTER,       numPartitions: 1, replicationFactor: 1 },
];

const existing = await admin.listTopics();
const missing = desired.filter((t) => !existing.includes(t.topic));

if (missing.length === 0) {
  logger.info('all topics already exist');
} else {
  await admin.createTopics({ topics: missing, waitForLeaders: true });
  logger.info({ created: missing.map((t) => t.topic) }, 'topics created');
}

await admin.disconnect();
