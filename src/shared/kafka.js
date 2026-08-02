import { Kafka, logLevel } from 'kafkajs';
import { config } from './config.js';
import { logger } from './logger.js';

export const TOPICS = {
  RAW_LOGS:          'raw-logs',
  ANALYSIS_REQUESTS: 'ai-analysis-requests',
  DIAGNOSED:         'diagnosed-incidents',
  DEAD_LETTER:       'dead-letter',
};

export function createKafka(clientId) {
  return new Kafka({
    clientId,
    brokers: config.KAFKA_BROKERS.split(',').map((b) => b.trim()),
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 8 },
  });
}

export async function createProducer(clientId) {
  const producer = createKafka(clientId).producer({ allowAutoTopicCreation: false });
  await producer.connect();
  logger.info({ clientId }, 'producer connected');
  return producer;
}

export async function runConsumer({ clientId, groupId, topics, handler }) {
  const consumer = createKafka(clientId).consumer({ groupId });
  await consumer.connect();
  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }
  logger.info({ clientId, groupId, topics }, 'consumer subscribed');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await handler({ topic, partition, message });
    },
  });

  return consumer;
}

export function onShutdown(fn) {
  let shuttingDown = false;
  const run = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutdown started');
    const force = setTimeout(() => {
      logger.error('shutdown timed out after 10s, forcing exit');
      process.exit(1);
    }, 10_000);
    force.unref();
    try {
      await fn();
      logger.info('shutdown complete');
      process.exit(0);
    } catch (err) {
      logger.error({ err }, 'shutdown failed');
      process.exit(1);
    }
  };
  process.on('SIGTERM', () => run('SIGTERM'));
  process.on('SIGINT',  () => run('SIGINT'));
}
