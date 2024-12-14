SELECT
    id,
    name,
    iso3,
    has_hdi,
    has_wfp
FROM {{ ref('stg_countries') }}
WHERE name IS NOT NULL
ORDER BY name

