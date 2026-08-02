CREATE TABLE IF NOT EXISTS incidents (
  id               uuid PRIMARY KEY,
  service_id       text NOT NULL,
  fingerprint      text NOT NULL,
  title            text NOT NULL,
  summary          text NOT NULL,
  probable_cause   text,
  suggested_fix    text,
  confidence       real,
  severity         text NOT NULL,
  occurrence_count int  NOT NULL,
  window_seconds   int  NOT NULL,
  sample_logs      jsonb NOT NULL,
  llm_model        text,
  llm_tokens       int,
  llm_latency_ms   int,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS incidents_service_created_idx
  ON incidents (service_id, created_at DESC);
CREATE INDEX IF NOT EXISTS incidents_fingerprint_created_idx
  ON incidents (fingerprint, created_at DESC);

CREATE TABLE IF NOT EXISTS dead_letters (
  id           uuid PRIMARY KEY,
  source_topic text NOT NULL,
  payload      jsonb NOT NULL,
  error        text NOT NULL,
  attempts     int  NOT NULL DEFAULT 0,
  created_at   timestamptz NOT NULL DEFAULT now()
);
