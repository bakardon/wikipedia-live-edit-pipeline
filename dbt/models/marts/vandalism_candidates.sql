{{ config(materialized='table') }}

-- Window function #3: 10-minute SLIDING window grouped by (page, ip_class).
-- Vandalism signature on Wikipedia: short bursts of anonymous edits from the
-- same /24 subnet, often followed by reverts. We surface (page, 10-min window)
-- pairs that match.

WITH ten_min_buckets AS (
    -- Round each edit's timestamp DOWN to the nearest 5-minute boundary, then
    -- expand to the two overlapping 10-min windows it belongs to.
    SELECT
        date_trunc('minute', ts) - (EXTRACT(MINUTE FROM ts)::int % 5) * interval '1 minute' AS window_start,
        page_id,
        ip_class_v4,
        is_anon,
        is_revert,
        edit_id
    FROM {{ ref('fact_edit') }}
    WHERE namespace = 0
      AND ts >= now() - interval '7 days'
),
agg AS (
    SELECT
        window_start,
        window_start + interval '10 minutes'                  AS window_end,
        page_id,
        COUNT(*) FILTER (WHERE is_anon)                       AS anon_edit_count,
        COUNT(*)                                              AS total_edit_count,
        COUNT(DISTINCT ip_class_v4) FILTER (WHERE is_anon)    AS distinct_ip_classes,
        SUM(CASE WHEN is_revert THEN 1 ELSE 0 END)::numeric
            / NULLIF(COUNT(*), 0)                             AS revert_share
    FROM ten_min_buckets
    GROUP BY window_start, page_id
)
SELECT
    a.window_start,
    a.window_end,
    a.page_id,
    p.page_title,
    p.wiki_id,
    w.wiki_db,
    a.anon_edit_count,
    a.total_edit_count,
    a.distinct_ip_classes,
    a.revert_share
FROM agg a
JOIN {{ ref('dim_page') }} p ON p.page_id = a.page_id
JOIN {{ ref('dim_wiki') }} w ON w.wiki_id = p.wiki_id
WHERE a.anon_edit_count >= 3      -- minimum burst size
   OR a.revert_share    >= 0.3    -- elevated revert share
