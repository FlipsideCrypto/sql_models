{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key='block_id || pool_name', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'thorchain_pool_block_balances']
  )
}}

SELECT DISTINCT
  bpd.block_timestamp,
  bpd.block_id,
  bpd.pool_name,
  rune_e8 / POW(10, 8) AS rune_amount,
  rune_e8 / POW(10, 8) * rune_usd AS rune_amount_usd,
  asset_e8 / POW(10, 8) AS asset_amount,
  asset_e8 / POW(10, 8) * asset_usd AS asset_amount_usd
FROM {{ ref('thorchain__block_pool_depths') }} bpd

JOIN {{ ref('thorchain__prices') }} p 
ON bpd.block_id = p.block_id AND bpd.pool_name = p.pool_name
