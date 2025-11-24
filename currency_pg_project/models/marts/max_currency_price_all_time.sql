SELECT
    currency_code,
    MAX(rate) AS max_rate
FROM {{ ref("stg_currency") }}
GROUP BY currency_code
ORDER BY currency_code