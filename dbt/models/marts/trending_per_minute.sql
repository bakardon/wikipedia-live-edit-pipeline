{{ config(materialized='table') }}

-- Window function #1: RANK() OVER (PARTITION BY minute ORDER BY edits DESC)
-- Surfaces the top edited pages within each 1-minute tumbling window across
-- all wikis. Filters to article namespace (0) so talk pages don't pollute.

WITH per_minute AS (
    SELECT
        minute_bucket,
        wiki_id,
        page_id,
        COUNT(*) AS edit_count
    FROM {{ ref('fact_edit') }}
    WHERE namespace = 0
      AND NOT is_bot
    GROUP BY minute_bucket, wiki_id, page_id
)
SELECT
    pm.minute_bucket,
    pm.wiki_id,
    w.wiki_db,
    pm.page_id,
    p.page_title,
    pm.edit_count,
    RANK()       OVER (PARTITION BY pm.minute_bucket ORDER BY pm.edit_count DESC) AS rank_in_minute,
    DENSE_RANK() OVER (PARTITION BY pm.minute_bucket, pm.wiki_id ORDER BY pm.edit_count DESC) AS rank_in_wiki_minute
FROM per_minute pm
JOIN {{ ref('dim_wiki') }} w ON w.wiki_id = pm.wiki_id
JOIN {{ ref('dim_page') }} p ON p.page_id = pm.page_id
