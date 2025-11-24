SELECT
    name_curr,
    currency_code,
    MAX(rate) AS max_rate
FROM {{ ref("stg_currency") }}
WHERE exchange_date >= CURRENT_DATE - INTERVAL '3 day'
GROUP BY currency_code, name_curr
ORDER BY currency_code
