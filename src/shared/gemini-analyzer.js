import { GoogleGenAI, Type } from '@google/genai';
import { analysisSchema, analyzeRequestSchema, buildPrompt, SEVERITIES } from './analyzer.js';
import { logger } from './logger.js';

// Gemini's responseSchema mirrors analysisSchema but is expressed in the
// SDK's own schema DSL (which resembles OpenAPI, not zod). Keeping the two
// side-by-side in the same file means a change to analysisSchema flags a
// visible reminder to update this table. If we generated one from the other
// automatically, the coupling would be more elegant but the failure mode
// (silent drift on missing fields) would be worse.
const GEMINI_RESPONSE_SCHEMA = {
  type: Type.OBJECT,
  properties: {
    title:          { type: Type.STRING, description: 'Short one-line headline (max ~80 chars) for an incident dashboard' },
    summary:        { type: Type.STRING, description: '2-3 sentence description of what is happening' },
    probable_cause: { type: Type.STRING, description: 'One sentence, the most likely underlying cause' },
    suggested_fix:  { type: Type.STRING, description: 'One sentence, the most concrete next action for an on-call engineer' },
    confidence:     { type: Type.NUMBER, description: 'A number between 0 and 1 reflecting diagnostic certainty' },
    severity:       { type: Type.STRING, enum: SEVERITIES, description: 'Impact level: low, medium, high, or critical' },
  },
  required: ['title', 'summary', 'probable_cause', 'suggested_fix', 'confidence', 'severity'],
  propertyOrdering: ['title', 'summary', 'probable_cause', 'suggested_fix', 'confidence', 'severity'],
};

// Gemini returns these transiently under load; the free tier hits 503 often
// during bursts. Retrying with backoff turns "sometimes fails" into "almost
// always eventually succeeds" without any code-path change for callers.
const RETRYABLE_CODES = [429, 503, 504];
const MAX_ATTEMPTS    = 6;

function isRetryable(err) {
  const msg = String(err?.message ?? err);
  return RETRYABLE_CODES.some((code) => msg.includes(`"code":${code}`));
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function createGeminiAnalyzer({ apiKey, model }) {
  if (!apiKey) throw new Error('createGeminiAnalyzer: apiKey is required');
  if (!model)  throw new Error('createGeminiAnalyzer: model is required');

  const client = new GoogleGenAI({ apiKey });

  async function analyze(request, { signal } = {}) {
    const req = analyzeRequestSchema.parse(request);
    const prompt = buildPrompt(req);

    let response;
    let lastErr;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        response = await client.models.generateContent({
          model,
          contents: prompt,
          config: {
            responseMimeType: 'application/json',
            responseSchema:   GEMINI_RESPONSE_SCHEMA,
            temperature:      0.2,
            abortSignal:      signal,
          },
        });
        break;
      } catch (err) {
        lastErr = err;
        if (!isRetryable(err) || attempt === MAX_ATTEMPTS) {
          throw new Error(`gemini: ${err?.message ?? err}`);
        }
        // Exponential backoff with jitter: 500ms → 1s → 2s → 4s → 8s (+0-500ms)
        const backoffMs = Math.round(500 * Math.pow(2, attempt - 1) + Math.random() * 500);
        logger.warn(
          { attempt, of: MAX_ATTEMPTS, backoffMs, code: (err?.message ?? '').match(/"code":(\d+)/)?.[1] },
          'gemini transient error — retrying',
        );
        await sleep(backoffMs);
      }
    }

    let parsed;
    try {
      parsed = JSON.parse(response.text);
    } catch (err) {
      throw new Error(`gemini: response was not valid JSON: ${response.text?.slice(0, 200)}`);
    }

    const analysis = analysisSchema.parse(parsed);
    const usage = response.usageMetadata ?? {};
    return {
      ...analysis,
      llm_model:  model,
      llm_tokens: usage.totalTokenCount ?? null,
    };
  }

  return { analyze };
}
