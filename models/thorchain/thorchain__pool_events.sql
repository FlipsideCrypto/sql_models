{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'pool_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.ASSET,
  e.STATUS
FROM {{source('thorchain_midgard', 'pool_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)