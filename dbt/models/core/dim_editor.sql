{{ config(materialized='table') }}

-- Like dim_page, editor_id is a deterministic 64-bit hash so it's stable
-- across rebuilds even though the dim is materialized as a full table.

SELECT
    ('x' || substr(md5(editor || '|' || editor_type), 1, 16))::bit(64)::bigint
                                       AS editor_id,
    editor                             AS editor_name,
    editor_type,
    -- Each anon "editor" string is a single IP, so its /24 class is constant.
    -- For users / bots ip_class_v4 is NULL.
    MAX(ip_class_v4)                   AS ip_class_v4,
    NOW()                              AS first_seen
FROM {{ ref('stg_edit') }}
WHERE editor IS NOT NULL
GROUP BY editor, editor_type
