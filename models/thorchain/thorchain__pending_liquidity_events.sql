{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'pending_liquidity_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.RUNE_TX as rune_tx_id,
  e.POOL as pool_name,
  e.PENDING_TYPE,
  e.RUNE_E8,
  e.ASSET_TX as asset_tx_id,
  e.ASSET_E8,
  e.RUNE_ADDR as rune_address,
  e.ASSET_ADDR as asset_address,
  e.ASSET_CHAIN as asset_blockchain
FROM {{source('thorchain_midgard', 'pending_liquidity_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)