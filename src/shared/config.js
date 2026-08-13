import { z } from 'zod';

const schema = z.object({
  NODE_ENV:           z.string().default('development'),
  LOG_LEVEL:          z.string().default('info'),
  KAFKA_BROKERS:      z.string().default('localhost:19092'),
  REDIS_URL:          z.string().default('redis://localhost:6379'),
  DATABASE_URL:       z.string().default('postgres://aether:aether@localhost:5434/aetherinsight'),
  API_PORT:           z.coerce.number().int().positive().default(3000),
  WINDOW_SECONDS:     z.coerce.number().int().positive().default(10),
  ALERT_THRESHOLD:    z.coerce.number().int().positive().default(50),
  CLAIM_TTL_SECONDS:  z.coerce.number().int().positive().default(900),
  CONTEXT_SAMPLES:    z.coerce.number().int().positive().default(20),
  GEMINI_API_KEY:     z.string().optional(),
  GEMINI_MODEL:       z.string().default('gemini-2.0-flash'),
});

export function loadConfig(env) {
  const result = schema.safeParse(env);
  if (!result.success) {
    throw new Error(`Invalid configuration:\n${JSON.stringify(result.error.flatten().fieldErrors, null, 2)}`);
  }
  return result.data;
}

export const config = loadConfig(process.env);
