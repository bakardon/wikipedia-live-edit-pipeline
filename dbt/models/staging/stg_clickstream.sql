{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'stg_clickstream') }}
    WHERE nav_count > 0
      AND target IS NOT NULL
)
SELECT
    referer,
    target,
    nav_type,
    nav_count,
    wiki_db,
    dump_month,
    ingested_at,
    referer IN (
        'other-search', 'other-external', 'other-internal',
        'other-empty',  'other-other'
    ) AS referer_is_external
FROM source
