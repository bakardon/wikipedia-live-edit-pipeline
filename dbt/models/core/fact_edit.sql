{{ config(
    materialized='incremental',
    unique_key='edit_id',
    on_schema_change='append_new_columns',
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_fact_edit_ts_brin ON {{ this }} USING BRIN (ts)",
      "CREATE INDEX IF NOT EXISTS idx_fact_edit_page_ts ON {{ this }} (page_id, ts)",
      "CREATE INDEX IF NOT EXISTS idx_fact_edit_editor  ON {{ this }} (editor_id, ts)",
      "CREATE INDEX IF NOT EXISTS idx_fact_edit_wiki_ts ON {{ this }} (wiki_id, ts)",
      "CREATE INDEX IF NOT EXISTS idx_fact_edit_minute  ON {{ this }} (minute_bucket)"
    ]
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_edit') }}
    {% if is_incremental() %}
      WHERE ingested_at > (
        SELECT COALESCE(MAX(ingested_at), '1970-01-01'::timestamptz) FROM {{ this }}
      )
    {% endif %}
)
SELECT
    s.edit_id,
    to_char(date_trunc('minute', s.ts), 'YYYYMMDDHH24MI')::BIGINT          AS time_id,
    s.minute_bucket,
    s.wiki_db                                                              AS wiki_id,
    ('x' || substr(md5(s.wiki_db || '|' || s.namespace::text || '|' || s.page_title), 1, 16))::bit(64)::bigint
                                                                           AS page_id,
    ('x' || substr(md5(s.editor || '|' || s.editor_type), 1, 16))::bit(64)::bigint
                                                                           AS editor_id,
    s.ts,
    s.parent_id                                                            AS parent_edit_id,
    s.bytes_changed,
    s.is_minor,
    s.is_bot,
    s.is_anon,
    s.is_revert,
    s.comment,
    s.editor_type,
    s.ip_class_v4,
    s.namespace,
    s.ingested_at
FROM source s
