{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'errata_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.ASSET_E8,
  e.RUNE_E8,
  e.IN_TX,
  e.ASSET
FROM {{source('thorchain_midgard', 'errata_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)