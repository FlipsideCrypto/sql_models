{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', date, currency)",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'terra', 'console']
) }}

WITH ORACLE AS (

    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS DATE,
        currency,
        symbol,
        AVG(luna_exchange_rate) AS oracle_exchange,
        AVG(price_usd) AS oracle_usd
    FROM
        {{ ref('terra__oracle_prices') }}
    WHERE
        block_timestamp > getdate() - INTERVAL '6 month'
        AND symbol = 'EUT'
{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ ref('terra__oracle_prices') }}
)
{% endif %}
    GROUP BY
        1,
        2,
        3
),
swaps AS (
    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS DATE,
        SUM(
            IFF(
                token_0_currency = 'LUNA',
                token_0_amount,
                token_1_amount
            )
        ) AS luna,
        SUM(
            IFF(
                token_0_currency = 'EUT',
                token_0_amount,
                token_1_amount
            )
        ) AS eut,
        eut / luna AS swap_exchange
    FROM
        {{ ref('terra__swaps') }}
    WHERE
        swap_pair IN (
            'EUT to LUNA',
            'LUNA to EUT'
        )
        AND block_timestamp > getdate() - INTERVAL '6 month'
{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(
            block_timestamp :: DATE
        )
    FROM
        {{ ref('terra__swaps') }}
)
{% endif %}
    GROUP BY
        1
)
SELECT
    o.date,
    o.currency,
    o.symbol,
    o.oracle_exchange,
    s.swap_exchange,
    swap_exchange / oracle_exchange AS "% of Peg"
FROM
    ORACLE o
    INNER JOIN swaps s
    ON (
        o.date = s.date
    )
ORDER BY
    DATE DESC
