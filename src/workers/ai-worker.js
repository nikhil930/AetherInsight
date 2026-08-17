import { runConsumer, createProducer, TOPICS, onShutdown } from '../shared/kafka.js';
import { decode } from '../shared/events.js';
import { createDb, closeDb } from '../shared/db.js';
import { createFakeAnalyzer } from '../shared/analyzer.js';
import { createGeminiAnalyzer } from '../shared/gemini-analyzer.js';
import { config } from '../shared/config.js';
import { logger } from '../shared/logger.js';
import { processRequest } from './ai-worker-logic.js';

const db = createDb(config.DATABASE_URL);
const producer = await createProducer('ai-worker');

// Analyzer selection: real Gemini if a key is configured, fake otherwise.
// Both implement the same analyze(request, {signal}) -> analysis port, so
// the worker code below is identical either way.
const analyzer = config.GEMINI_API_KEY
  ? createGeminiAnalyzer({ apiKey: config.GEMINI_API_KEY, model: config.GEMINI_MODEL })
  : createFakeAnalyzer();

logger.info(
  { analyzer: config.GEMINI_API_KEY ? 'gemini' : 'fake', model: config.GEMINI_MODEL },
  'analyzer configured',
);

const deps = { analyzer, db, producer, logger };

const consumer = await runConsumer({
  clientId: 'ai-worker',
  groupId:  'ai-workers',
  topics:   [TOPICS.ANALYSIS_REQUESTS],
  handler: async ({ partition, message }) => {
    let event;
    try {
      event = decode(message.value);
    } catch (err) {
      logger.error({ err, partition, offset: message.offset }, 'undecodable message dropped');
      return;
    }

    try {
      await processRequest(deps, event);
    } catch (err) {
      logger.error({ err, incidentId: event.id }, 'unrecoverable error in ai-worker');
    }
  },
});

onShutdown(async () => {
  await consumer.disconnect();
  await producer.disconnect();
  await closeDb(db);
});
