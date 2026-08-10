import Fastify from 'fastify';
import { pathToFileURL } from 'node:url';
import { encode, rawLogSchema } from '../shared/events.js';
import { TOPICS, createProducer, onShutdown } from '../shared/kafka.js';
import { config } from '../shared/config.js';
import { logger } from '../shared/logger.js';

export async function createApp({ producer }) {
  const app = Fastify({ logger: false });

  app.post('/ingest', async (req, reply) => {
    const parsed = rawLogSchema.safeParse(req.body);
    if (!parsed.success) {
      return reply.code(400).send({ error: parsed.error.flatten().fieldErrors });
    }
    const value = encode('log.raw', parsed.data);
    const { id } = JSON.parse(value);
    await producer.send({
      topic: TOPICS.RAW_LOGS,
      messages: [{ key: parsed.data.service_id, value }],
    });
    return reply.code(202).send({ id });
  });

  app.get('/healthz', async () => ({ ok: true }));

  return app;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const producer = await createProducer('ingestion-api');
  const app = await createApp({ producer });
  await app.listen({ port: config.API_PORT, host: '0.0.0.0' });
  logger.info({ port: config.API_PORT }, 'ingestion API listening');

  onShutdown(async () => {
    await app.close();
    await producer.disconnect();
  });
}
