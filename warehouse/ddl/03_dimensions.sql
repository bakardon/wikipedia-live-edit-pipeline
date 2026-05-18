-- ============================================================================
-- Dimensions (core schema)
-- All dim tables EXCEPT dim_time are built by dbt models from raw.* sources.
-- dim_time is pre-seeded here because its values are deterministic and the
-- streaming/batch jobs don't need to populate it.
-- ============================================================================

CREATE TABLE core.dim_time (
    time_id      BIGINT      PRIMARY KEY,                                -- YYYYMMDDHHMI as integer
    ts           TIMESTAMPTZ NOT NULL UNIQUE,
    date         DATE        NOT NULL,
    hour         SMALLINT    NOT NULL,
    minute       SMALLINT    NOT NULL,
    day_of_week  SMALLINT    NOT NULL,                                   -- 0=Sun..6=Sat (Postgres EXTRACT(DOW))
    is_weekend   BOOLEAN     NOT NULL
);

COMMENT ON TABLE core.dim_time IS
    'Minute-grain time dimension. Pre-seeded for 2026 to avoid lookup-time gaps.';

INSERT INTO core.dim_time (time_id, ts, date, hour, minute, day_of_week, is_weekend)
SELECT
    to_char(g, 'YYYYMMDDHH24MI')::BIGINT,
    g,
    g::DATE,
    EXTRACT(HOUR   FROM g)::SMALLINT,
    EXTRACT(MINUTE FROM g)::SMALLINT,
    EXTRACT(DOW    FROM g)::SMALLINT,
    EXTRACT(DOW    FROM g) IN (0, 6)
FROM generate_series(
    timestamptz '2026-01-01 00:00:00 UTC',
    timestamptz '2026-12-31 23:59:00 UTC',
    interval '1 minute'
) AS g
ON CONFLICT DO NOTHING;
