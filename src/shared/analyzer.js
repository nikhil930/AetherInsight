import { z } from 'zod';

export const SEVERITIES = ['low', 'medium', 'high', 'critical'];

export const analysisSchema = z.object({
  title:          z.string().min(1).max(200),
  summary:        z.string().min(1).max(2000),
  probable_cause: z.string().min(1).max(2000),
  suggested_fix:  z.string().min(1).max(2000),
  confidence:     z.number().min(0).max(1),
  severity:       z.enum(SEVERITIES),
  llm_model:      z.string().optional(),
  llm_tokens:     z.number().int().nonnegative().optional(),
});

export const analyzeRequestSchema = z.object({
  fingerprint:      z.string().min(1),
  service_id:       z.string().min(1),
  occurrence_count: z.number().int().positive(),
  window_seconds:   z.number().int().positive(),
  sample_logs:      z.array(z.string()).min(1),
});

export function buildPrompt(request) {
  const req = analyzeRequestSchema.parse(request);
  const samples = req.sample_logs.slice(0, 5).map((s, i) => `${i + 1}. ${s}`).join('\n');
  return [
    `You are a site-reliability engineer diagnosing a production incident.`,
    ``,
    `Service: ${req.service_id}`,
    `Error fingerprint: ${req.fingerprint}`,
    `Occurrences: ${req.occurrence_count} in the last ${req.window_seconds} seconds`,
    ``,
    `Sample log lines from this error cluster:`,
    samples,
    ``,
    `Return a JSON object with exactly these fields:`,
    `- title: a short one-line headline (max ~80 chars) suitable for an incident dashboard`,
    `- summary: 2-3 sentence description of what is happening`,
    `- probable_cause: one sentence, the most likely underlying cause`,
    `- suggested_fix: one sentence, the most concrete next action an on-call engineer should take`,
    `- confidence: a number between 0 and 1 reflecting how sure you are of the diagnosis`,
    `- severity: one of "low", "medium", "high", "critical" based on likely user impact`,
  ].join('\n');
}

function severityFromCount(count) {
  if (count >= 500) return 'critical';
  if (count >= 200) return 'high';
  if (count >= 50)  return 'medium';
  return 'low';
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
      title:          `Fake incident ${shortFp} in ${req.service_id}`,
      summary:        `Fake analyzer detected ${req.occurrence_count} occurrences of fingerprint ${shortFp} in ${req.service_id} over ${req.window_seconds}s. This is a deterministic fake for testing.`,
      probable_cause: `Fake probable cause for fingerprint ${shortFp} in service ${req.service_id}`,
      suggested_fix:  `Fake fix for fingerprint ${shortFp} (based on ${req.occurrence_count} occurrences)`,
      confidence:     0.75,
      severity:       severityFromCount(req.occurrence_count),
    };
    return analysisSchema.parse(analysis);
  }

  return { analyze };
}
