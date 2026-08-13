import { describe, it, expect } from 'vitest';
import {
  analysisSchema,
  analyzeRequestSchema,
  buildPrompt,
  createFakeAnalyzer,
} from '../src/shared/analyzer.js';

function makeRequest(overrides = {}) {
  return {
    fingerprint: 'abc123def456',
    service_id:  'payments-api',
    error_count: 87,
    first_seen:  '2026-08-13T10:00:00Z',
    last_seen:   '2026-08-13T10:00:12Z',
    sample_logs: [
      'ECONNREFUSED connecting to postgres at 10.0.1.5:5432',
      'ECONNREFUSED connecting to postgres at 10.0.1.5:5432',
      'ECONNREFUSED connecting to postgres at 10.0.1.5:5432',
    ],
    ...overrides,
  };
}

describe('buildPrompt', () => {
  it('embeds service_id, fingerprint, count, and samples', () => {
    const prompt = buildPrompt(makeRequest());
    expect(prompt).toContain('payments-api');
    expect(prompt).toContain('abc123def456');
    expect(prompt).toContain('87');
    expect(prompt).toContain('ECONNREFUSED');
  });

  it('caps samples at 5 to control prompt size', () => {
    const req = makeRequest({
      sample_logs: Array.from({ length: 20 }, (_, i) => `log line ${i}`),
    });
    const prompt = buildPrompt(req);
    expect(prompt).toContain('1. log line 0');
    expect(prompt).toContain('5. log line 4');
    expect(prompt).not.toContain('6. log line 5');
  });

  it('rejects invalid requests at the boundary', () => {
    expect(() => buildPrompt({ fingerprint: 'x' })).toThrow();
  });
});

describe('createFakeAnalyzer', () => {
  it('returns a valid analysis matching the schema', async () => {
    const analyzer = createFakeAnalyzer();
    const analysis = await analyzer.analyze(makeRequest());
    expect(() => analysisSchema.parse(analysis)).not.toThrow();
    expect(analysis.confidence).toBeGreaterThanOrEqual(0);
    expect(analysis.confidence).toBeLessThanOrEqual(1);
  });

  it('produces deterministic output for the same fingerprint', async () => {
    const analyzer = createFakeAnalyzer();
    const a = await analyzer.analyze(makeRequest());
    const b = await analyzer.analyze(makeRequest());
    expect(a).toEqual(b);
  });

  it('produces different output for different fingerprints', async () => {
    const analyzer = createFakeAnalyzer();
    const a = await analyzer.analyze(makeRequest({ fingerprint: 'aaaaaaaa' }));
    const b = await analyzer.analyze(makeRequest({ fingerprint: 'bbbbbbbb' }));
    expect(a.root_cause).not.toEqual(b.root_cause);
  });

  it('injects latency when delayMs is set', async () => {
    const analyzer = createFakeAnalyzer({ delayMs: 50 });
    const start = Date.now();
    await analyzer.analyze(makeRequest());
    const elapsed = Date.now() - start;
    expect(elapsed).toBeGreaterThanOrEqual(45);
  });

  it('fails every Nth call when failEvery is set', async () => {
    const analyzer = createFakeAnalyzer({ failEvery: 3 });
    await expect(analyzer.analyze(makeRequest())).resolves.toBeDefined();
    await expect(analyzer.analyze(makeRequest())).resolves.toBeDefined();
    await expect(analyzer.analyze(makeRequest())).rejects.toThrow(/fake analyzer failure/);
    await expect(analyzer.analyze(makeRequest())).resolves.toBeDefined();
  });

  it('honors AbortSignal to cancel in-flight calls', async () => {
    const analyzer = createFakeAnalyzer({ delayMs: 500 });
    const controller = new AbortController();
    const promise = analyzer.analyze(makeRequest(), { signal: controller.signal });
    setTimeout(() => controller.abort(), 20);
    await expect(promise).rejects.toThrow(/aborted/);
  });

  it('rejects invalid requests at the boundary', async () => {
    const analyzer = createFakeAnalyzer();
    await expect(analyzer.analyze({ fingerprint: 'x' })).rejects.toThrow();
  });
});
