{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'bond_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.TX as tx_id,
  e.TO_ADDR as to_addres,
  e.FROM_ADDR as from_address,
  e.MEMO,
  e.ASSET,
  e.CHAIN as blockchain,
  e.BOND_TYPE,
  e.E8,
  e.ASSET_E8
FROM {{source('thorchain_midgard', 'bond_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)