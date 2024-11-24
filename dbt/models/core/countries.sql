WITH country_union AS (
    SELECT
        DISTINCT 
        CASE
            WHEN country = 'Iran (Islamic Republic of)' THEN 'Iran'
            WHEN country = 'State of Palestine' THEN 'Palestine'
            ELSE TRIM(
          REGEXP_REPLACE(
              REGEXP_REPLACE(country, '[^a-zA-Z0-9\s''\-\(\)]', '', 'g'),
              '\s+', ' ', 'g'
          )
        ) END AS name,
        iso3,
        1 AS has_hdi,
        0 AS has_wfp
    FROM {{ source('core', 'raw_hdi') }}

    UNION

    SELECT
        DISTINCT 
        CASE
            WHEN adm0_name = 'Iran (Islamic Republic of)' THEN 'Iran'
            WHEN adm0_name = 'State of Palestine' THEN 'Palestine'
            ELSE TRIM(
              REGEXP_REPLACE(
                  REGEXP_REPLACE(adm0_name, '[^a-zA-Z0-9\s''\-\(\)]', '', 'g'),
                  '\s+', ' ', 'g'
              )
            ) END AS name,
        NULL AS iso3,
        0 AS has_hdi,
        1 AS has_wfp
    FROM {{ source('core', 'raw_wfp') }}
),
deduplicated_countries AS (
    SELECT 
        name,
        MAX(has_hdi) AS has_hdi,
        MAX(has_wfp) AS has_wfp,
        MAX(iso3) AS iso3
    FROM country_union
    GROUP BY name
)
SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS id,
    name,
    iso3,
    has_hdi::INTEGER AS has_hdi,
    has_wfp::INTEGER AS has_wfp
FROM deduplicated_countries
ORDER BY name
