{{ config(
    materialized = 'view',
    tags = ['snowflake', 'terra', 'console', 'terraswap_top_swaps']
) }}

WITH NATIVE AS (

    SELECT
        REPLACE(
            swap_pair,
            ' to ',
            '-'
        ) AS swap_pair,
        COUNT(
            DISTINCT tx_id
        ) AS tradeCount,
        COUNT(
            DISTINCT trader
        ) AS addressCount,
        SUM(NVL(token_0_amount, 0)) AS volumetokenamount,
        SUM(NVL(token_0_amount_usd, 0)) AS volumetoken0usd
    FROM
        {{ ref('terra__swaps') }}
    WHERE
        swap_pair IS NOT NULL
    GROUP BY
        swap_pair
    ORDER BY
        volumetoken0usd DESC
    LIMIT
        5
)
SELECT
    *
FROM
    NATIVE
