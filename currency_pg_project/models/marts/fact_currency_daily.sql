SELECT
    name_curr,
    currency_code,
    exchange_date,
    AVG(rate) AS avg_rate
FROM {{ ref("stg_currency") }}
GROUP BY 1, 2, 3


