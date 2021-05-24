{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'transfer_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.ASSET,
  e.AMOUNT_E8,
  e.FROM_ADDR as from_address,
  e.TO_ADDR as to_address
FROM {{source('thorchain_midgard', 'transfer_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)