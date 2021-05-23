{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'message_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.ACTION,
  e.FROM_ADDR as from_address
FROM {{source('thorchain_midgard', 'message_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
