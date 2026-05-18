-- ============================================================================
-- Indexes (DDL-managed)
-- Only raw.* indexes here. Indexes on dbt-built tables (core.fact_edit etc.)
-- are added via dbt model post_hooks so they survive table rebuilds.
-- ============================================================================

-- raw.stg_edit
CREATE INDEX idx_stg_edit_ts        ON raw.stg_edit USING BRIN (ts);
CREATE INDEX idx_stg_edit_page      ON raw.stg_edit (wiki_db, page_title, ts);
CREATE INDEX idx_stg_edit_editor    ON raw.stg_edit (editor, ts) WHERE is_anon;
CREATE INDEX idx_stg_edit_ingested  ON raw.stg_edit (ingested_at);

-- raw.stg_clickstream
CREATE INDEX idx_stg_cs_target      ON raw.stg_clickstream (wiki_db, target);
