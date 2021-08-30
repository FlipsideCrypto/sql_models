{{ config(
  materialized = 'incremental',
  unique_key = 'symbol || hour',
  incremental_strategy = 'delete+insert',
  cluster_by = ['hour'],
  tags = ['snowflake', 'terra_gold', 'terra_prices']
) }}

WITH hours AS (

  SELECT
    HOUR
  FROM
    {{ source(
      'shared',
      'hours'
    ) }}
  WHERE
    TRUE

{% if is_incremental() %}
AND HOUR >= getdate() - INTERVAL '3 days'
AND HOUR < getdate() + INTERVAL '1 day'
{% endif %}
),
raw_prices AS (
  SELECT
    symbol,
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS HOUR,
    AVG(price) AS price
  FROM
    {{ source(
      'shared',
      'prices'
    ) }}
  WHERE
    asset_id IN (
      4172,
      -- Luna
      5115,
      -- Terra KRW (KRT)
      6370 -- Terra SDT (SDR)
    )

{% if is_incremental() %}
AND recorded_at >= getdate() - INTERVAL '3 days'
{% endif %}
AND recorded_at < getdate() + INTERVAL '1 day'
GROUP BY
  symbol,
  HOUR
UNION ALL
SELECT
  symbol,
  DATE_TRUNC(
    'hour',
    recorded_at
  ) AS HOUR,
  AVG(price) AS price
FROM
  {{ source(
    'shared',
    'coingecko_prices'
  ) }}
WHERE
  asset_id IN ('terrausd')

{% if is_incremental() %}
AND recorded_at >= getdate() - INTERVAL '3 days'
{% endif %}
AND recorded_at < getdate() + INTERVAL '1 day'
GROUP BY
  symbol,
  HOUR
),
assets AS (
  SELECT
    symbol
  FROM
    raw_prices
  GROUP BY
    1
),
hour_assets AS (
  SELECT
    h.hour,
    A.symbol
  FROM
    hours h
    CROSS JOIN assets A
)
SELECT
  ha.hour,
  ha.symbol,
  -- use price if we have other, otherwise fall back to daily average
  COALESCE(rp.price, AVG(rp.price) over(PARTITION BY DATE_TRUNC('day', ha.hour), ha.symbol)) AS price
FROM
  hour_assets ha
  LEFT OUTER JOIN raw_prices rp
  ON ha.symbol = rp.symbol
  AND ha.hour = rp.hour
