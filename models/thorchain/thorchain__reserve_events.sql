{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'reserve_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.TX as tx_id,
  e.CHAIN as blockchain,
  e.ADDR as address,
  e.E8,
  e.ASSET_E8,
  e.FROM_ADDR as from_address,
  e.TO_ADDR	as to_address,
  e.MEMO,
  e.ASSET
FROM {{source('thorchain_midgard', 'reserve_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)