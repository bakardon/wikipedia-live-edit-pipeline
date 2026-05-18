{{ config(
    materialized='table',
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_fact_cs_target     ON {{ this }} (target_page_id)",
      "CREATE INDEX IF NOT EXISTS idx_fact_cs_wiki_month ON {{ this }} (wiki_id, dump_month)"
    ]
) }}

SELECT
    row_number() OVER (ORDER BY cs.dump_month, cs.wiki_db, cs.target, cs.referer)  AS fc_id,
    cs.wiki_db                                                                     AS wiki_id,
    ('x' || substr(md5(cs.wiki_db || '|0|' || cs.target),  1, 16))::bit(64)::bigint
                                                                                   AS target_page_id,
    CASE
        WHEN cs.referer_is_external THEN NULL
        ELSE ('x' || substr(md5(cs.wiki_db || '|0|' || cs.referer), 1, 16))::bit(64)::bigint
    END                                                                            AS referer_page_id,
    cs.nav_type,
    cs.nav_count,
    cs.dump_month
FROM {{ ref('stg_clickstream') }} cs
