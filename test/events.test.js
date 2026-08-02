import { describe, it, expect } from 'vitest';
import { encode, decode, SCHEMA_VERSION } from '../src/shared/events.js';

const validLog = { service_id: 'payments', level: 'error', message: 'db timeout' };

describe('encode', () => {
  it('wraps a payload in a versioned envelope', () => {
    const parsed = JSON.parse(encode('log.raw', validLog));
    expect(parsed.v).toBe(SCHEMA_VERSION);
    expect(parsed.type).toBe('log.raw');
    expect(parsed.id).toMatch(/^[0-9a-f-]{36}$/);
    expect(parsed.payload.service_id).toBe('payments');
  });

  it('rejects a payload that fails its schema', () => {
    expect(() => encode('log.raw', { service_id: 'x', level: 'nonsense', message: 'y' })).toThrow();
  });

  it('rejects an unknown event type', () => {
    expect(() => encode('log.imaginary', validLog)).toThrow(/Unknown event type/);
  });
});

describe('decode', () => {
  it('round-trips an encoded message', () => {
    const out = decode(Buffer.from(encode('log.raw', validLog)));
    expect(out.payload.message).toBe('db timeout');
  });

  it('rejects an unsupported envelope version', () => {
    const bad = JSON.stringify({
      v: 99, type: 'log.raw', id: '00000000-0000-4000-8000-000000000000',
      ts: new Date().toISOString(), payload: validLog,
    });
    expect(() => decode(bad)).toThrow(/Unsupported envelope version/);
  });
});
