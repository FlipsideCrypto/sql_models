{{ config(
    materialized = 'view',
    unique_key = "CONCAT_WS('-', METRIC_DATE, EVENT_CURRENCY)",
    tags = ['snowflake', 'terra', 'console']
) }}

WITH tmp AS (

    SELECT
        DATE,
        currency,
        SUM(balance_usd) AS staked_balance
    FROM
        {{ ref('terra__daily_balances') }}
    WHERE
        DATE >= CURRENT_DATE - 60
        AND balance_type = 'staked'
        AND balance > 0
    GROUP BY
        DATE,
        currency
    ORDER BY
        DATE,
        currency
),
tmp_2 AS (
    SELECT
        block_timestamp :: DATE AS metric_date,
        currency AS event_currency,
        SUM(event_amount_usd) AS volume
    FROM
        {{ ref('terra__reward') }}
    WHERE
        block_timestamp :: DATE >= CURRENT_DATE - 60
        AND currency IN(
            'KRT',
            'LUNA',
            'SDT',
            'UST'
        )
    GROUP BY
        metric_date,
        event_currency
)
SELECT
    t2.metric_date,
    t2.event_currency,
    (
        t2.volume / t.staked_balance
    ) * 100 AS volume
FROM
    tmp_2 t2
    LEFT JOIN tmp t
    ON t2.metric_date = t.date
WHERE t2.metric_date <> current_date
ORDER BY
    metric_date,
    event_currency
