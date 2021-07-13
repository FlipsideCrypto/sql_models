{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp', 
    unique_key=["block_id", "block_timestamp", "asset"], 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'thorchain', 'bond_actions']
  )
}}

WITH block_prices AS (
  SELECT
    AVG(rune_usd) AS rune_usd,
    block_id
  FROM {{ ref('thorchain__prices') }}
  GROUP BY block_id
)
SELECT
  be.block_timestamp,
  be.block_id,
  tx_id,
  from_address,
  to_addres,
  asset,
  blockchain,
  bond_type,
  e8 / POW(10, 8) AS asset_amount,
  rune_usd * asset_amount / POW(10, 8) AS asset_usd
FROM {{ ref('thorchain__bond_events') }} be
JOIN block_prices p 
ON be.block_id = p.block_id