import { describe, it, expect, beforeEach } from 'vitest';
import { createApp } from '../src/api/server.js';

function fakeProducer() {
  const sent = [];
  return {
    sent,
    send: async ({ topic, messages }) => {
      for (const m of messages) sent.push({ topic, key: m.key, value: m.value });
    },
  };
}

let producer;
let app;

beforeEach(async () => {
  producer = fakeProducer();
  app = await createApp({ producer });
});

describe('POST /ingest', () => {
  it('accepts a valid log and returns 202 with an event id', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/ingest',
      payload: { service_id: 'checkout', level: 'error', message: 'db timeout' },
    });
    expect(res.statusCode).toBe(202);
    expect(JSON.parse(res.body)).toHaveProperty('id');
  });

  it('produces to raw-logs keyed by service_id', async () => {
    await app.inject({
      method: 'POST',
      url: '/ingest',
      payload: { service_id: 'checkout', level: 'error', message: 'boom' },
    });
    expect(producer.sent).toHaveLength(1);
    expect(producer.sent[0].topic).toBe('raw-logs');
    expect(producer.sent[0].key).toBe('checkout');
  });

  it('sends a valid envelope (v, type, id, ts, payload)', async () => {
    await app.inject({
      method: 'POST',
      url: '/ingest',
      payload: { service_id: 'api', level: 'warn', message: 'slow query' },
    });
    const envelope = JSON.parse(producer.sent[0].value);
    expect(envelope.v).toBe(1);
    expect(envelope.type).toBe('log.raw');
    expect(envelope.payload.service_id).toBe('api');
    expect(envelope.payload.level).toBe('warn');
    expect(envelope.payload.message).toBe('slow query');
  });

  it('rejects a missing service_id with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/ingest',
      payload: { level: 'error', message: 'no service' },
    });
    expect(res.statusCode).toBe(400);
    expect(producer.sent).toHaveLength(0);
  });

  it('rejects an invalid level with 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/ingest',
      payload: { service_id: 'api', level: 'BAD', message: 'x' },
    });
    expect(res.statusCode).toBe(400);
    expect(producer.sent).toHaveLength(0);
  });

  it('returns 400 when the body is not JSON', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/ingest',
      headers: { 'content-type': 'application/json' },
      payload: 'not-json',
    });
    expect(res.statusCode).toBe(400);
    expect(producer.sent).toHaveLength(0);
  });
});

describe('GET /healthz', () => {
  it('returns 200 with ok:true', async () => {
    const res = await app.inject({ method: 'GET', url: '/healthz' });
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ ok: true });
  });
});
