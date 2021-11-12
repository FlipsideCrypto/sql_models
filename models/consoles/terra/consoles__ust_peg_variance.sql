-- Velocity: 8dcd4c26-575b-4a9a-9b00-a70cf21bc4d7
{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', date, currency)",
    tags = ['snowflake', 'terra', 'console_a']
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
        AND symbol = 'UST'
    GROUP BY
        DATE,
        currency,
        symbol
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
                token_0_currency = 'UST',
                token_0_amount,
                token_1_amount
            )
        ) AS ust,
        ust / luna AS swap_exchange
    FROM
        {{ ref('terra__swaps') }}
    WHERE
        swap_pair IN (
            'UST to LUNA',
            'LUNA to UST'
        )
        AND trader <> 'terra10kjnhhsgm4jfakr85673and3aw2y4a03598e0m'
        AND block_timestamp > getdate() - INTERVAL '6 month'
    GROUP BY
        DATE
)
SELECT
    ORACLE.date,
    ORACLE.currency,
    ORACLE.symbol,
    ORACLE.oracle_exchange,
    swaps.swap_exchange,
    swap_exchange / oracle_exchange AS percent_of_peg
FROM
    ORACLE
    INNER JOIN swaps
    ON (
        ORACLE.date = swaps.date
    )
ORDER BY
    DATE DESC
