{{ config(
  materialized = 'view',
  unique_key = "CONCAT_WS('-', METRIC_DATE, currency)",
  tags = ['snowflake', 'terra', 'console']
) }}

SELECT
  DATE_TRUNC(
    'day',
    block_timestamp
  ) AS metric_date,
  currency,
  symbol,
  AVG(price_usd) AS "AVG"
FROM
  {{ ref('terra__oracle_prices') }}
WHERE
  block_timestamp :: DATE > CURRENT_DATE - 180
  AND symbol = 'UST'
  AND price_usd > 0
GROUP BY
  metric_date,
  currency,
  symbol
ORDER BY
  metric_date DESC
