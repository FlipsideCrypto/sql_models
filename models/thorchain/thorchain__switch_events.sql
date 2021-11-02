{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'switch_events']
  )
}}

SELECT
  _FIVETRAN_ID AS identified_id,
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.BURN_ASSET,
  e.BURN_E8,
  e.TO_ADDR	as to_address,
  e.FROM_ADDR as from_address
FROM {{source('thorchain_midgard', 'switch_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)
GROUP BY identified_id,block_timestamp,block_id,BURN_ASSET,BURN_E8,to_address,from_address