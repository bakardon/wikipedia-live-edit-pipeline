{{ config(materialized='table') }}

-- Window function #2: LAG(edit_count, 60) OVER (PARTITION BY page ORDER BY minute)
-- Compares this minute's edit count for a page against the same page 60 minutes
-- ago. Acceleration > 0 means edit-rate is rising — a stronger breaking-news
-- signal than raw volume because viral pages also have a quiet baseline.

WITH per_minute AS (
    SELECT
        minute_bucket,
        page_id,
        wiki_id,
        COUNT(*) AS edit_count
    FROM {{ ref('fact_edit') }}
    WHERE namespace = 0
    GROUP BY minute_bucket, page_id, wiki_id
)
SELECT
    pm.minute_bucket,
    pm.page_id,
    p.page_title,
    pm.wiki_id,
    w.wiki_db,
    pm.edit_count                                                      AS edits_this_min,
    LAG(pm.edit_count, 60) OVER (PARTITION BY pm.page_id ORDER BY pm.minute_bucket)
                                                                       AS edits_lag_60,
    pm.edit_count
        - COALESCE(
            LAG(pm.edit_count, 60) OVER (PARTITION BY pm.page_id ORDER BY pm.minute_bucket),
            0
          )                                                            AS accel
FROM per_minute pm
JOIN {{ ref('dim_page') }} p ON p.page_id = pm.page_id
JOIN {{ ref('dim_wiki') }} w ON w.wiki_id = pm.wiki_id
