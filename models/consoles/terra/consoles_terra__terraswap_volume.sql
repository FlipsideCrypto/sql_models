{{ config(
    materialized = 'view',
    tags = ['snowflake', 'terra', 'console', 'terraswap_volume']
) }}

WITH prices AS (

    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS dayz,
        currency,
        AVG(price_usd) * 1 AS avgg
    FROM
        {{ ref('terra__oracle_prices') }}
    WHERE
        dayz >= CURRENT_DATE - 90
    GROUP BY
        dayz,
        currency
    ORDER BY
        dayz DESC
),
nonnative AS (
    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS dayzz,
        COUNT(
            DISTINCT tx_id
        ) AS n_trades,
        COUNT(DISTINCT (msg_value :sender :: STRING)) AS n_traders,
        msg_value :execute_msg :swap :offer_asset :info :native_token :denom :: STRING AS swap_currency,
        SUM(
            msg_value :execute_msg :swap :offer_asset :amount / pow(
                10,
                6
            )
        ) AS trading_vol_token0,
        C.address AS contract_label
    FROM
        {{ ref('terra__msgs') }}
        LEFT OUTER JOIN {{ ref('terra__labels') }} C
        ON msg_value :contract :: STRING = C.address
    WHERE
        msg_value :execute_msg :swap IS NOT NULL
        AND contract_label IS NOT NULL
        AND tx_status = 'SUCCEEDED'
    GROUP BY
        dayzz,
        swap_currency,
        contract_label
    ORDER BY
        dayzz DESC
),
combine AS (
    SELECT
        *
    FROM
        nonnative
        LEFT JOIN prices
        ON nonnative.dayzz = prices.dayz
        AND nonnative.swap_currency = prices.currency
)
SELECT
    dayzz,
    n_trades,
    n_traders,
    --currency,
    contract_label,
    (
        avgg * trading_vol_token0
    ) AS usdadj
FROM
    combine
WHERE
    usdadj > 0
    AND dayz >= CURRENT_DATE - 90
