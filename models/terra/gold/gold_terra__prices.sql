{{ config(
  materialized = 'incremental',
  unique_key = 'symbol || hour',
  incremental_strategy = 'delete+insert',
  cluster_by = ['hour, 'symbol'],
  tags = ['snowflake', 'terra_gold', 'terra_prices']
) }}

WITH hours AS (
  select hour
  from
  {{source('shared', 'hours')}}
  where
  {% if is_incremental() %}
    hour >= getdate() - interval '3 days'  and hour < getdate() + interval '1 day'
  {% endif %}
),
raw_prices AS (
  SELECT symbol, date_trunc('hour', recorded_at) as hour, avg(price) as price FROM {{source('shared','prices')}}  WHERE asset_id IN (
    4172, -- Luna
    5115, -- Terra KRW (KRT)
    6370 -- Terra SDT (SDR)
  )
  {% if is_incremental() %}
    AND recorded_at >= getdate() - interval '3 days'
  {% endif %}
  AND recorded_at < getdate() + interval '1 day'
  GROUP BY symbol, hour

  UNION ALL

  SELECT symbol, date_trunc('hour', recorded_at) as hour, avg(price) as price
  FROM {{source('shared','coingecko_prices')}} WHERE asset_id IN ('terrausd')
  {% if is_incremental() %}
    AND recorded_at >= getdate() - interval '3 days'
  {% endif %}
  AND recorded_at < getdate() + interval '1 day'
  GROUP BY symbol, hour
),
assets AS (
  SELECT symbol FROM raw_prices GROUP BY 1
),
hour_assets AS (
  SELECT
  h.hour,
  a.symbol
  FROM
  hours h
  CROSS JOIN
  assets a
)
SELECT
ha.hour,
ha.symbol,
-- use price if we have other, otherwise fall back to daily average
coalesce(rp.price, avg(rp.price) over(partition by date_trunc('day', ha.hour), ha.symbol)) as price
FROM
hour_assets ha
LEFT OUTER JOIN
raw_prices rp
ON
ha.symbol = rp.symbol
AND
ha.hour = rp.hour