{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["block_id", "pool_name"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'thorchain_prices']
  )
}}

-- block level prices by pool
-- step 1 what is the USD pool with the highest balance (aka deepest pool)
WITH max_pool_blocks AS (
  SELECT 
    max(block_id) AS max_block, 
    pool_name
  FROM {{ ref('thorchain__block_pool_depths') }} 
  WHERE pool_name in ('BNB.USDT-6D8', 'BNB.BUSD-BD1', 'ETH.USDT-0XDAC17F958D2EE523A2206206994597C13D831EC7')
    {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
  GROUP BY pool_name
),

reference_pool AS (
  SELECT 
    block_id, 
    block_timestamp, 
    bpd.pool_name, 
    rune_e8
  FROM {{ ref('thorchain__block_pool_depths') }}  bpd
  JOIN max_pool_blocks mpb 
  ON bpd.pool_name = mpb.pool_name AND bpd.block_id = mpb.max_block
  WHERE
    {% if is_incremental() %}
    bpd.block_timestamp >= getdate() - interval '2 days'
    {% else %}
    bpd.block_timestamp >= getdate() - interval '9 months'
    {% endif %}
  ORDER BY rune_e8 DESC
  LIMIT 1
),

-- step 2 use that pool to determine the price of rune
rune_usd AS (
  SELECT
    block_timestamp,
    block_id,
    asset_e8 / rune_e8 AS rune_usd
  FROM {{ ref('thorchain__block_pool_depths') }} bpd
  WHERE pool_name = (SELECT pool_name FROM reference_pool)
    {% if is_incremental() %}
    AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
    AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
)

-- step 3 calculate the prices of assets by pool, in terms of tokens per tokens
-- and in USD for both tokens
SELECT
  bpd.block_id,
  bpd.block_timestamp,
  rune_e8 / asset_e8 AS price_rune_asset,
  asset_e8 / rune_e8 AS price_asset_rune,
  rune_usd * (rune_e8 / asset_e8) AS asset_usd,
  rune_usd,
  pool_name
FROM {{ ref('thorchain__block_pool_depths') }}  bpd
JOIN rune_usd ru ON bpd.block_id = ru.block_id
WHERE rune_e8 > 0 AND asset_e8 > 0
