import { describe, it, expect } from 'vitest';
import { loadConfig } from '../src/shared/config.js';

describe('loadConfig', () => {
  it('applies defaults when env is empty', () => {
    const cfg = loadConfig({});
    expect(cfg.KAFKA_BROKERS).toBe('localhost:19092');
    expect(cfg.ALERT_THRESHOLD).toBe(50);
    expect(cfg.WINDOW_SECONDS).toBe(10);
  });

  it('coerces numeric strings to numbers', () => {
    const cfg = loadConfig({ ALERT_THRESHOLD: '5', API_PORT: '4000' });
    expect(cfg.ALERT_THRESHOLD).toBe(5);
    expect(cfg.API_PORT).toBe(4000);
  });

  it('throws on a non-numeric threshold', () => {
    expect(() => loadConfig({ ALERT_THRESHOLD: 'banana' })).toThrow();
  });
});
