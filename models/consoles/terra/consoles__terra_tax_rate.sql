{{ config(
  materialized = 'view',
  unique_key = 'METRIC_DATE',
  tags = ['snowflake', 'terra', 'console']
) }}



WITH tax_rate AS (
    SELECT
        date_trunc('day', block_timestamp) AS date,
        avg(tax_rate) AS tax_rate
    FROM
        {{ ref('silver_terra__tax_rate') }}
    where block_timestamp::date >= CURRENT_DATE - 90
    GROUP BY
    date

),
price AS (
    SELECT
        date_trunc('day', recorded_at) AS metric_date,
        avg(price) AS price
    FROM
        {{ source(
      'shared',
      'prices_v2'
    ) }}
    WHERE
        asset_id = '6370'
    and recorded_at::date >= CURRENT_DATE - 90
GROUP BY
        metric_date
)


SELECT
    date as metric_date,
    avg(tax_rate) * 100 as metric_value,
    -- Default Tax Rate
    avg(effective_tax_rate) * 100 as metric_value_2 -- Effective Tax Rate
FROM
    (
        SELECT
            date_trunc('day', e.block_timestamp) AS date,
            tax_rate,
            CASE
                WHEN e.event_amount_usd <= p.price / t.tax_rate THEN t.tax_rate
                ELSE p.price / e.event_amount_usd
            END AS effective_tax_rate
        FROM
            terra.transfers e
            JOIN tax_rate t ON date_trunc('day', e.block_timestamp) = t.date
            JOIN price p ON date_trunc('day', e.block_timestamp) = p.metric_date
        WHERE
            e.event_amount > 0
    ) sq
GROUP BY
    date
ORDER BY
    metric_date DESC


