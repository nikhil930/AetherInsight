import pg from 'pg';

const { Pool } = pg;

export function createDb(url) {
  return new Pool({ connectionString: url });
}

export async function closeDb(pool) {
  await pool.end();
}

const INSERT_INCIDENT_SQL = `
  INSERT INTO incidents (
    id, service_id, fingerprint,
    title, summary, probable_cause, suggested_fix,
    confidence, severity,
    occurrence_count, window_seconds, sample_logs,
    llm_model, llm_tokens, llm_latency_ms
  )
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $14, $15)
  ON CONFLICT (id) DO NOTHING
  RETURNING id
`;

export async function insertIncident(pool, incident) {
  const {
    id, service_id, fingerprint,
    title, summary, probable_cause, suggested_fix,
    confidence, severity,
    occurrence_count, window_seconds, sample_logs,
    llm_model, llm_tokens, llm_latency_ms,
  } = incident;

  const { rows } = await pool.query(INSERT_INCIDENT_SQL, [
    id, service_id, fingerprint,
    title, summary, probable_cause ?? null, suggested_fix ?? null,
    confidence ?? null, severity,
    occurrence_count, window_seconds, JSON.stringify(sample_logs),
    llm_model ?? null, llm_tokens ?? null, llm_latency_ms ?? null,
  ]);

  return { inserted: rows.length === 1, id };
}

const SELECT_INCIDENT_BY_ID_SQL = `
  SELECT id, service_id, fingerprint,
         title, summary, probable_cause, suggested_fix,
         confidence, severity,
         occurrence_count, window_seconds, sample_logs,
         llm_model, llm_tokens, llm_latency_ms,
         created_at
    FROM incidents
   WHERE id = $1
`;

export async function getIncident(pool, id) {
  const { rows } = await pool.query(SELECT_INCIDENT_BY_ID_SQL, [id]);
  return rows[0] ?? null;
}

const LIST_INCIDENTS_SQL = `
  SELECT id, service_id, fingerprint,
         title, summary, severity,
         occurrence_count, window_seconds,
         created_at
    FROM incidents
   WHERE ($1::text IS NULL OR service_id = $1)
   ORDER BY created_at DESC
   LIMIT $2 OFFSET $3
`;

export async function listIncidents(pool, { service_id = null, limit = 50, offset = 0 } = {}) {
  const { rows } = await pool.query(LIST_INCIDENTS_SQL, [service_id, limit, offset]);
  return rows;
}
