{{ config(materialized='view') }}

-- Cleans + enriches raw.stg_edit:
--   - filters edits with no edit_id
--   - derives editor_type (user / anon / bot) and ip_class_v4 (/24 for anon ipv4)
--   - adds minute_bucket for window functions

WITH source AS (
    SELECT * FROM {{ source('raw', 'stg_edit') }}
    WHERE edit_id IS NOT NULL
      AND wiki_db IS NOT NULL
      AND page_title IS NOT NULL
)
SELECT
    edit_id,
    rev_id,
    parent_id,
    ts,
    date_trunc('minute', ts)                   AS minute_bucket,
    page_title,
    namespace,
    wiki_db,
    editor,
    is_anon,
    is_bot,
    is_minor,
    bytes_changed,
    comment,
    raw_payload,
    ingested_at,
    CASE
        WHEN is_bot  THEN 'bot'
        WHEN is_anon THEN 'anon'
        ELSE              'user'
    END                                        AS editor_type,
    CASE
        WHEN is_anon AND editor ~ '^(\d{1,3}\.){3}\d{1,3}$'
            THEN regexp_replace(editor, '\.\d+$', '.0/24')
        ELSE NULL
    END                                        AS ip_class_v4,
    -- Reverts can be inferred from comments — Wikipedia edit comments contain
    -- "Reverted", "(undo)", or "rollback" when an edit reverts a prior one.
    (comment ~* '\m(reverted|undo|rollback)\M')::boolean AS is_revert
FROM source
