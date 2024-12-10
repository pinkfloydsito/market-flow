{{ config(materialized='table') }}

SELECT
    DISTINCT adm1_id AS id,
    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(adm1_name, '[^a-zA-Z0-9\s''\-\(\)]', '', 'g'),
            '\s+', ' ', 'g'
        )
    ) AS name,
    (
        SELECT id
        FROM {{ ref('countries') }}
        WHERE name = TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    CASE
                        WHEN cleaned_wfp.adm0_name = 'Iran  (Islamic Republic of)' THEN 'Iran'
                        WHEN cleaned_wfp.adm0_name = 'State of Palestine' THEN 'Palestine'
                        ELSE cleaned_wfp.adm0_name
                    END, '[^a-zA-Z0-9\s''\-\(\)]', '', 'g'),
                '\s+', ' ', 'g'
            )
        )
        LIMIT 1
    ) AS country_id,
    NULL::double precision AS latitude,
    NULL::double precision AS longitude
FROM {{ ref('cleaned_wfp') }} as cleaned_wfp
WHERE adm1_id IS NOT NULL AND adm1_name IS NOT NULL
ORDER BY name
