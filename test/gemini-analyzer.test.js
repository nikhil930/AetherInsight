import { describe, it, expect } from 'vitest';
import { createGeminiAnalyzer } from '../src/shared/gemini-analyzer.js';
import { analysisSchema, SEVERITIES } from '../src/shared/analyzer.js';
import { config } from '../src/shared/config.js';

const HAS_KEY = Boolean(config.GEMINI_API_KEY);

function makeRequest(overrides = {}) {
  return {
    fingerprint:      'a'.repeat(40),
    service_id:       'payments-api',
    occurrence_count: 87,
    window_seconds:   10,
    sample_logs: [
      'ECONNREFUSED connecting to postgres at 10.0.1.5:5432',
      'ECONNREFUSED connecting to postgres at 10.0.1.5:5432',
      'ECONNREFUSED connecting to postgres at 10.0.1.5:5432',
    ],
    ...overrides,
  };
}

describe('createGeminiAnalyzer (construction)', () => {
  it('throws when apiKey is missing', () => {
    expect(() => createGeminiAnalyzer({ model: 'gemini-2.0-flash' })).toThrow(/apiKey/);
  });

  it('throws when model is missing', () => {
    expect(() => createGeminiAnalyzer({ apiKey: 'fake' })).toThrow(/model/);
  });

  it('constructs successfully with both fields', () => {
    const analyzer = createGeminiAnalyzer({ apiKey: 'fake-key', model: 'gemini-2.0-flash' });
    expect(analyzer).toHaveProperty('analyze');
    expect(typeof analyzer.analyze).toBe('function');
  });
});

describe.skipIf(!HAS_KEY)('createGeminiAnalyzer (live API — requires GEMINI_API_KEY)', () => {
  const analyzer = HAS_KEY
    ? createGeminiAnalyzer({ apiKey: config.GEMINI_API_KEY, model: config.GEMINI_MODEL })
    : null;

  it('returns a valid analysis matching the DB-aligned schema', async () => {
    const analysis = await analyzer.analyze(makeRequest());
    expect(() => analysisSchema.parse(analysis)).not.toThrow();
    expect(SEVERITIES).toContain(analysis.severity);
    expect(analysis.confidence).toBeGreaterThanOrEqual(0);
    expect(analysis.confidence).toBeLessThanOrEqual(1);
    expect(analysis.title.length).toBeGreaterThan(0);
  }, 30_000);

  it('populates llm_model and llm_tokens from usageMetadata', async () => {
    const analysis = await analyzer.analyze(makeRequest());
    expect(analysis.llm_model).toBe(config.GEMINI_MODEL);
    expect(analysis.llm_tokens).toBeGreaterThan(0);
  }, 30_000);

  it('wraps SDK errors with a "gemini:" prefix (using bogus key)', async () => {
    const bad = createGeminiAnalyzer({ apiKey: 'INVALID-KEY-FOR-TESTING', model: config.GEMINI_MODEL });
    await expect(bad.analyze(makeRequest())).rejects.toThrow(/^gemini:/);
  }, 30_000);
});
