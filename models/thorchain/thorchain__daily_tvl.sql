{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["block_timestamp", "pool_name"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'daily_tvl']
  )
}}

--daily_earnings
WITH max_daily_block AS (
  SELECT 
    max(block_id) AS block_id,
    date_trunc('day', block_timestamp) AS day
  FROM {{ ref('thorchain__prices') }}
  GROUP BY day
),
daily_rune_price AS (
  SELECT
    p.block_id,
    day,
    AVG(rune_usd) AS rune_usd
    FROM {{ ref('thorchain__prices') }} p
  JOIN max_daily_block mdb WHERE p.block_id = mdb.block_id
  GROUP BY day, p.block_id
)
SELECT
br.day,
total_value_pooled / POW(10, 8) AS total_value_pooled,
total_value_pooled / POW(10, 8) * rune_usd AS total_value_pooled_usd,
total_value_bonded / POW(10, 8) AS total_value_bonded,
total_value_bonded / POW(10, 8) * rune_usd AS total_value_bonded_usd,
total_value_locked / POW(10, 8) AS total_value_locked,
total_value_locked / POW(10, 8) * rune_usd AS total_value_locked_usd
FROM {{ ref('thorchain__total_value_locked') }} br
JOIN daily_rune_price drp ON br.day = drp.day