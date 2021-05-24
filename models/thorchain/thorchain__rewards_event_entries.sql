{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'rewards_event_entries']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.POOL as pool_name,
  e.RUNE_E8
FROM {{source('thorchain_midgard', 'rewards_event_entries')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)