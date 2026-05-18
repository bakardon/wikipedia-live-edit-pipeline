{{ config(materialized='table') }}

WITH all_wikis AS (
    SELECT DISTINCT wiki_db FROM {{ ref('stg_edit') }}
    UNION
    SELECT DISTINCT wiki_db FROM {{ ref('stg_clickstream') }}
)
SELECT
    wiki_db                                                            AS wiki_id,
    wiki_db,
    -- "enwiki" → "en", "dewiktionary" → "de"
    regexp_replace(wiki_db,
        '(wiktionary|wikibooks|wikinews|wikiquote|wikisource|wikiversity|wikivoyage|wiki)$',
        '')                                                            AS language,
    CASE
        WHEN wiki_db LIKE '%wiktionary'  THEN 'wiktionary'
        WHEN wiki_db LIKE '%wikibooks'   THEN 'wikibooks'
        WHEN wiki_db LIKE '%wikinews'    THEN 'wikinews'
        WHEN wiki_db LIKE '%wikiquote'   THEN 'wikiquote'
        WHEN wiki_db LIKE '%wikisource'  THEN 'wikisource'
        WHEN wiki_db LIKE '%wikiversity' THEN 'wikiversity'
        WHEN wiki_db LIKE '%wikivoyage'  THEN 'wikivoyage'
        ELSE 'wikipedia'
    END                                                                AS family,
    NOW()                                                              AS created_at
FROM all_wikis
WHERE wiki_db IS NOT NULL
