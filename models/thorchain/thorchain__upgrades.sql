{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["block_id", "from_address", "to_address", "burn_asset"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'thorchain_upgrades']
  )
}}

--total_block_rewards
WITH block_prices AS (
  SELECT
    AVG(rune_usd) AS rune_usd,
    block_id
    FROM {{ ref('thorchain__prices') }}
    GROUP BY block_id
)

SELECT
  block_timestamp,
  se.block_id,
  from_address,
  to_address,
  burn_asset,
  burn_e8 / POW(10, 8) AS rune_amount,
  burn_e8 / POW(10, 8) * rune_usd AS rune_amount_usd
FROM {{ ref('thorchain__switch_events') }} se

JOIN block_prices p 
ON se.block_id = p.block_id