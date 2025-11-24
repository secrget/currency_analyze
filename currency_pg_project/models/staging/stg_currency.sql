SELECT
    id,
    CAST(digital_code AS INTEGER) AS digital_code,
    name_curr,
    CAST(rate AS FLOAT) AS rate,
    letter_code_curr AS currency_code,
    exchange_date
FROM {{ source("public","nbu_exchange_rate")}}

