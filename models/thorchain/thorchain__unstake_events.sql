{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'unstake_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.TX as tx_id,
  e.CHAIN as blockchain,
  e.POOL as pool_name,
  e.TO_ADDR as to_address,
  e.FROM_ADDR as from_address,
  e.ASSET,
  e.EMIT_RUNE_E8,
  e.ASYMMETRY,
  e.ASSET_E8,
  e.STAKE_UNITS,
  e.MEMO,
  e.EMIT_ASSET_E8,
  e.IMP_LOSS_PROTECTION_E8,
  e.BASIS_POINTS
FROM {{source('thorchain_midgard', 'unstake_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)