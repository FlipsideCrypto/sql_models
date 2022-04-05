{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'set_version_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.NODE_ADDR as node_address,
  e.VERSION
FROM {{source('thorchain_midgard', 'set_version_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)