{{ config(materialized='table') }}

-- page_id is a deterministic 64-bit hash of (wiki_db, namespace, page_title)
-- so values are stable across rebuilds — fact_edit's incremental rows stay
-- referentially valid even when dim_page is fully rebuilt.

WITH stream_pages AS (
    SELECT DISTINCT
        wiki_db,
        namespace,
        page_title
    FROM {{ ref('stg_edit') }}
),
clickstream_pages AS (
    -- Targets (always page titles)
    SELECT DISTINCT wiki_db, 0 AS namespace, target AS page_title
    FROM {{ ref('stg_clickstream') }}
    UNION
    -- Referers (skip the synthetic "other-*" sources)
    SELECT DISTINCT wiki_db, 0 AS namespace, referer AS page_title
    FROM {{ ref('stg_clickstream') }}
    WHERE NOT referer_is_external
),
all_pages AS (
    SELECT * FROM stream_pages
    UNION
    SELECT * FROM clickstream_pages
)
SELECT
    ('x' || substr(md5(wiki_db || '|' || namespace::text || '|' || page_title), 1, 16))::bit(64)::bigint
                                            AS page_id,
    wiki_db                                 AS wiki_id,
    page_title,
    namespace,
    NOW()                                   AS first_seen
FROM all_pages
WHERE page_title IS NOT NULL
  AND wiki_db    IS NOT NULL
