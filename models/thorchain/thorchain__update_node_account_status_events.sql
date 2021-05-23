{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'update_node_account_status_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.NODE_ADDR as node_address,
  e."CURRENT" as current_status,
  e.FORMER as former_status
FROM {{source('thorchain_midgard', 'update_node_account_status_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)