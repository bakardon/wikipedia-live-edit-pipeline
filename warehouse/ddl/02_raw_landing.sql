-- ============================================================================
-- Raw landing tables — direct sinks for streaming + batch jobs.
-- Denormalized on purpose: streaming stays a single INSERT per event with no
-- dim upserts in the hot path. dbt promotes these into core.* dim+fact tables.
-- ============================================================================

-- Streaming sink: one row per Wikipedia edit observed in the recentchange firehose.
CREATE TABLE raw.stg_edit (
    edit_id         BIGINT      NOT NULL,                                -- Wikipedia revision id
    rev_id          BIGINT,
    parent_id       BIGINT,
    ts              TIMESTAMPTZ NOT NULL,
    page_title      TEXT        NOT NULL,
    namespace       INTEGER     NOT NULL,                                -- 0=article, 1=talk, ...
    wiki_db         TEXT        NOT NULL,                                -- enwiki, dewiki, ...
    editor          TEXT        NOT NULL,                                -- username (users/bots) or IP (anons)
    is_anon         BOOLEAN     NOT NULL,
    is_bot          BOOLEAN     NOT NULL,
    is_minor        BOOLEAN     NOT NULL,
    bytes_changed   INTEGER,
    comment         TEXT,
    raw_payload     JSONB,                                               -- original event for replay/debug
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (edit_id, ts)
);

COMMENT ON TABLE raw.stg_edit IS
    'Append-only landing for the Wikimedia recentchange SSE stream. Source of truth for replay.';

-- Batch sink: monthly Wikipedia Clickstream dumps.
-- Schema: https://dumps.wikimedia.org/other/clickstream/readme.html
CREATE TABLE raw.stg_clickstream (
    referer         TEXT NOT NULL,                                       -- "other-search", "other-external", or page title
    target          TEXT NOT NULL,
    nav_type        TEXT NOT NULL,                                       -- link, external, other
    nav_count       INTEGER NOT NULL,
    wiki_db         TEXT NOT NULL,                                       -- enwiki for our scope
    dump_month      DATE NOT NULL,                                       -- first day of the dump month
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raw.stg_clickstream IS
    'Append-only landing for Wikipedia Clickstream monthly dumps (referer→target navigation counts).';
