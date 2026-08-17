import { randomUUID } from 'node:crypto';
import { encode } from '../shared/events.js';
import { insertIncident, insertDeadLetter } from '../shared/db.js';
import { TOPICS } from '../shared/kafka.js';

// Processes ONE analysis.requested event end-to-end.
//
// Idempotency chain (why the same message twice is safe):
//   1. Filter worker's Redis claim (SET NX EX) prevents most duplicate escalations.
//   2. If a duplicate does slip through (claim TTL expired, restart, etc.),
//      the envelope id from the ORIGINAL escalation is reused as the incident id.
//   3. Postgres ON CONFLICT (id) DO NOTHING catches it at the storage layer.
//      Second write returns inserted:false; we skip producing to diagnosed-incidents
//      so the dashboard is not double-notified.
export async function processRequest(deps, event) {
  const { analyzer, db, producer, logger } = deps;
  const payload = event.payload;
  const incidentId = event.id;

  let analysis;
  const startedAt = Date.now();
  try {
    analysis = await analyzer.analyze(payload);
  } catch (err) {
    logger.error(
      { err, incidentId, fingerprint: payload.fingerprint, service: payload.service_id },
      'analyzer failed — routing to dead_letters',
    );
    await insertDeadLetter(db, {
      id:           randomUUID(),
      source_topic: TOPICS.ANALYSIS_REQUESTS,
      payload:      event,
      error:        `analyzer: ${err.message}`,
    });
    return { action: 'failed', reason: 'analyzer', incidentId };
  }
  const latencyMs = Date.now() - startedAt;

  const incident = {
    id:               incidentId,
    service_id:       payload.service_id,
    fingerprint:      payload.fingerprint,
    title:            analysis.title,
    summary:          analysis.summary,
    probable_cause:   analysis.probable_cause,
    suggested_fix:    analysis.suggested_fix,
    confidence:       analysis.confidence,
    severity:         analysis.severity,
    occurrence_count: payload.occurrence_count,
    window_seconds:   payload.window_seconds,
    sample_logs:      payload.sample_logs,
    llm_model:        analysis.llm_model ?? null,
    llm_tokens:       analysis.llm_tokens ?? null,
    llm_latency_ms:   latencyMs,
  };

  let result;
  try {
    result = await insertIncident(db, incident);
  } catch (err) {
    logger.error(
      { err, incidentId, fingerprint: payload.fingerprint },
      'db insert failed — routing to dead_letters',
    );
    await insertDeadLetter(db, {
      id:           randomUUID(),
      source_topic: TOPICS.ANALYSIS_REQUESTS,
      payload:      event,
      error:        `db: ${err.message}`,
    });
    return { action: 'failed', reason: 'db', incidentId };
  }

  if (!result.inserted) {
    logger.info(
      { incidentId, fingerprint: payload.fingerprint },
      'duplicate escalation — incident already persisted, skipping produce',
    );
    return { action: 'duplicate', incidentId };
  }

  await producer.send({
    topic: TOPICS.DIAGNOSED,
    messages: [{
      key: payload.service_id,
      value: encode('incident.diagnosed', {
        incident_id:      incidentId,
        service_id:       payload.service_id,
        fingerprint:      payload.fingerprint,
        title:            analysis.title,
        summary:          analysis.summary,
        severity:         analysis.severity,
        confidence:       analysis.confidence,
        occurrence_count: payload.occurrence_count,
        window_seconds:   payload.window_seconds,
      }),
    }],
  });

  logger.warn(
    { incidentId, fingerprint: payload.fingerprint, severity: incident.severity, service: payload.service_id },
    'incident diagnosed',
  );
  return { action: 'diagnosed', incidentId, incident };
}
