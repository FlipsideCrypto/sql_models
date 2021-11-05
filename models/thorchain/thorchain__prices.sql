{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key="block_id || '-' || pool_name", 
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
rune_usd_max_tbl AS (
  SELECT
    block_timestamp,
    block_id,
    asset_e8 / rune_e8 AS rune_usd_max
  FROM {{ ref('thorchain__block_pool_depths') }} bpd
  WHERE pool_name = (SELECT pool_name FROM reference_pool)
    {% if is_incremental() %}
    AND bpd.block_timestamp >= getdate() - interval '2 days'
    {% else %}
    AND bpd.block_timestamp >= getdate() - interval '9 months'
    {% endif %}
),

rune_usd_sup_tbl AS (
  SELECT 
    block_timestamp,
    block_id,
    AVG(rune_usd) AS rune_usd_sup
  FROM (
    SELECT
      block_timestamp,
      block_id,
      asset_e8 / rune_e8 AS rune_usd
    FROM {{ ref('thorchain__block_pool_depths') }} bpd
    WHERE rune_e8 > 0 AND asset_e8 > 0
      {% if is_incremental() %}
      AND bpd.block_timestamp >= getdate() - interval '2 days'
      {% else %}
      AND bpd.block_timestamp >= getdate() - interval '9 months'
      {% endif %}
  )
  GROUP BY block_timestamp,block_id
),

rune_usd AS (
  SELECT 
    block_timestamp,
    block_id,
    CASE WHEN rune_usd_max IS NULL THEN lag(rune_usd_max) ignore nulls over (order by block_id) ELSE rune_usd_max END AS rune_usd
  FROM (
    SELECT
      COALESCE(a.block_timestamp, b.block_timestamp) AS block_timestamp,
      COALESCE(a.block_id, b.block_id) AS block_id,
      rune_usd_max
    FROM rune_usd_max_tbl a
    FULL JOIN rune_usd_sup_tbl b
    ON a.block_timestamp = b.block_timestamp AND a.block_id = b.block_id
  )
)

-- step 3 calculate the prices of assets by pool, in terms of tokens per tokens
-- and in USD for both tokens
SELECT DISTINCT
  bpd.block_id,
  bpd.block_timestamp,
  COALESCE(rune_e8 / asset_e8, 0) AS price_rune_asset,
  COALESCE(asset_e8 / rune_e8, 0) AS price_asset_rune,
  COALESCE(rune_usd * (rune_e8 / asset_e8), 0) AS asset_usd,
  COALESCE(rune_usd, 0) AS rune_usd,
  pool_name
FROM {{ ref('thorchain__block_pool_depths') }}  bpd
JOIN rune_usd ru ON bpd.block_id = ru.block_id
WHERE rune_e8 > 0 AND asset_e8 > 0
