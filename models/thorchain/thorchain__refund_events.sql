{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'refund_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.TX as tx_id,
  e.ASSET_E8,
  e.MEMO,
  e.REASON,
  e.ASSET_2ND_E8,
  e.CODE,
  e.CHAIN as blockchain,
  e.ASSET,
  e.ASSET_2ND,
  e.TO_ADDR	as to_address,
  e.FROM_ADDR as from_address
FROM {{source('thorchain_midgard', 'refund_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)