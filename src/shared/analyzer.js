import { z } from 'zod';

export const analysisSchema = z.object({
  root_cause:    z.string().min(1),
  suggested_fix: z.string().min(1),
  confidence:    z.number().min(0).max(1),
});

export const analyzeRequestSchema = z.object({
  fingerprint: z.string().min(1),
  service_id:  z.string().min(1),
  error_count: z.number().int().positive(),
  first_seen:  z.string().min(1),
  last_seen:   z.string().min(1),
  sample_logs: z.array(z.string()).min(1),
});

export function buildPrompt(request) {
  const req = analyzeRequestSchema.parse(request);
  const samples = req.sample_logs.slice(0, 5).map((s, i) => `${i + 1}. ${s}`).join('\n');
  return [
    `You are a site-reliability engineer diagnosing a production incident.`,
    ``,
    `Service: ${req.service_id}`,
    `Error fingerprint: ${req.fingerprint}`,
    `Occurrences: ${req.error_count} between ${req.first_seen} and ${req.last_seen}`,
    ``,
    `Sample log lines from this error cluster:`,
    samples,
    ``,
    `Return a JSON object with three fields:`,
    `- root_cause: one sentence, the most likely underlying cause`,
    `- suggested_fix: one sentence, the most concrete next action`,
    `- confidence: a number between 0 and 1 reflecting how sure you are`,
  ].join('\n');
}

export function createFakeAnalyzer(opts = {}) {
  const { delayMs = 0, failEvery = 0 } = opts;
  let callCount = 0;

  async function analyze(request, { signal } = {}) {
    const req = analyzeRequestSchema.parse(request);
    callCount += 1;
    const thisCall = callCount;

    if (delayMs > 0) {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(resolve, delayMs);
        if (signal) {
          signal.addEventListener('abort', () => {
            clearTimeout(timer);
            reject(new Error('aborted'));
          }, { once: true });
        }
      });
    }

    if (failEvery > 0 && thisCall % failEvery === 0) {
      throw new Error(`fake analyzer failure (call ${thisCall})`);
    }

    const shortFp = req.fingerprint.slice(0, 8);
    const analysis = {
      root_cause:    `Fake root cause for fingerprint ${shortFp} in service ${req.service_id}`,
      suggested_fix: `Fake fix for fingerprint ${shortFp} (based on ${req.error_count} occurrences)`,
      confidence:    0.75,
    };
    return analysisSchema.parse(analysis);
  }

  return { analyze };
}
