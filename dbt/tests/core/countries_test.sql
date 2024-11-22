SELECT name, COUNT(*)
FROM {{ ref('countries') }}
GROUP BY name
HAVING COUNT(*) > 1
