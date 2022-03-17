{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'swap_events']
  )
}}

SELECT
    _FIVETRAN_ID AS unique_id,
    to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
    bl.height as block_id,
    e.TX as tx_id,
    e.CHAIN as blockchain,
    e.TO_ADDR as to_address,
    e.FROM_ADDR as from_address,
    e.TO_ASSET,
    e.FROM_ASSET,
    e.SWAP_SLIP_BP,
    e.LIQ_FEE_IN_RUNE_E8,
    e.LIQ_FEE_E8,    
    e.TO_E8,
    e.POOL as pool_name,
    e.MEMO,
    e.TO_E8_MIN,
    e.FROM_E8
FROM {{source('thorchain_midgard', 'swap_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)