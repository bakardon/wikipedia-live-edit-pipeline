-- ============================================================================
-- Wikipedia Edit Pipeline — physical schema
-- Layer: storage
-- ============================================================================
-- Three schemas separate concerns:
--   raw   — append-only landing zone (streaming + batch). Source of truth
--           for replay; cheap to truncate and reload from Kafka or dump.
--   core  — Kimball star schema (dim_* + fact_*). Built by dbt from raw.
--   marts — analytical models (window functions, aggregates). Built by dbt
--           from core; what the Streamlit dashboard reads.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;

COMMENT ON SCHEMA raw   IS 'Append-only landing zone for streaming + batch ingest.';
COMMENT ON SCHEMA core  IS 'Star-schema warehouse (dim_* + fact_*), built by dbt from raw.';
COMMENT ON SCHEMA marts IS 'dbt analytical models (window functions, aggregates) read by the dashboard.';
