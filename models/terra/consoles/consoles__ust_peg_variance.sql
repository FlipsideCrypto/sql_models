-- Velocity: 8dcd4c26-575b-4a9a-9b00-a70cf21bc4d7
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
        AND symbol = 'UST'

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
    DATE,
    currency,
    oracle_exchange
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
        terra.swaps {{ ref('terra__swaps') }}
    WHERE
        swap_pair IN (
            'UST to LUNA',
            'LUNA to UST'
        )
        AND trader <> 'terra10kjnhhsgm4jfakr85673and3aw2y4a03598e0m'
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
    DATE
)
SELECT
    ORACLE.date,
    ORACLE.currency,
    ORACLE.symbol,
    ORACLE.oracle_exchange,
    swaps.swap_exchange,
    swap_exchange / oracle_exchange AS "% of Peg"
FROM
    ORACLE
    INNER JOIN swaps
    ON (
        ORACLE.date = swaps.date
    )
ORDER BY
    DATE DESC
