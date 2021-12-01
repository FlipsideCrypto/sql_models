{{ config(
    materialized = 'view',
    unique_key = 'METRIC_DATE',
    tags = ['snowflake', 'terra', 'console']
) }}

WITH tax_rate AS (

    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS DATE,
        AVG(tax_rate) AS tax_rate
    FROM
        {{ ref('silver_terra__tax_rate') }}
    WHERE
        block_timestamp :: DATE >= CURRENT_DATE - 90
    GROUP BY
        DATE
),
price AS (
    SELECT
        DATE_TRUNC(
            'day',
            recorded_at
        ) AS metric_date,
        AVG(price) AS price
    FROM
        {{ source(
            'shared',
            'prices_v2'
        ) }}
    WHERE
        asset_id = '6370'
        AND recorded_at :: DATE >= CURRENT_DATE - 90
    GROUP BY
        metric_date
)
SELECT
    DATE AS metric_date,
    AVG(tax_rate) * 100 AS metric_value,
    AVG(effective_tax_rate) * 100 AS metric_value_2
FROM
    (
        SELECT
            DATE_TRUNC(
                'day',
                e.block_timestamp
            ) AS DATE,
            tax_rate,
            CASE
                WHEN e.event_amount_usd <= p.price / t.tax_rate THEN t.tax_rate
                ELSE p.price / e.event_amount_usd
            END AS effective_tax_rate
        FROM
            terra.transfers e
            JOIN tax_rate t
            ON DATE_TRUNC(
                'day',
                e.block_timestamp
            ) = t.date
            JOIN price p
            ON DATE_TRUNC(
                'day',
                e.block_timestamp
            ) = p.metric_date
        WHERE
            e.event_amount > 0
    ) sq
GROUP BY
    DATE
ORDER BY
    metric_date DESC
