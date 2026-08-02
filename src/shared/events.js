import { z } from 'zod';
import { randomUUID } from 'node:crypto';

export const SCHEMA_VERSION = 1;

export const rawLogSchema = z.object({
  service_id: z.string().min(1).max(64),
  level:      z.enum(['debug', 'info', 'warn', 'error', 'fatal']),
  message:    z.string().min(1).max(8192),
  stack:      z.string().max(16384).optional(),
  trace_id:   z.string().max(128).optional(),
  meta:       z.record(z.unknown()).optional(),
});

export const analysisRequestSchema = z.object({
  service_id:       z.string().min(1),
  fingerprint:      z.string().length(40),
  occurrence_count: z.number().int().positive(),
  window_seconds:   z.number().int().positive(),
  sample_logs:      z.array(z.string()).min(1),
});

const PAYLOADS = {
  'log.raw':            rawLogSchema,
  'analysis.requested': analysisRequestSchema,
};

function schemaFor(type) {
  const schema = PAYLOADS[type];
  if (!schema) throw new Error(`Unknown event type: ${type}`);
  return schema;
}

export function encode(type, payload) {
  return JSON.stringify({
    v: SCHEMA_VERSION,
    type,
    id: randomUUID(),
    ts: new Date().toISOString(),
    payload: schemaFor(type).parse(payload),
  });
}

const envelopeSchema = z.object({
  v:    z.number().int(),
  type: z.string(),
  id:   z.string().uuid(),
  ts:   z.string(),
  payload: z.unknown(),
});

export function decode(input) {
  const outer = envelopeSchema.parse(JSON.parse(input.toString()));
  if (outer.v !== SCHEMA_VERSION) {
    throw new Error(`Unsupported envelope version ${outer.v} (expected ${SCHEMA_VERSION})`);
  }
  return { ...outer, payload: schemaFor(outer.type).parse(outer.payload) };
}
