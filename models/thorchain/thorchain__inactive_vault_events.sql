{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'gas_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.ADD_ASGARD_ADDR as add_asgard_address
FROM {{source('thorchain_midgard', 'inactive_vault_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
