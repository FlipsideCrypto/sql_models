{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'slash_amounts']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.ASSET_E8,
  e.POOL as pool_name,
  e.ASSET
FROM {{source('thorchain_midgard', 'slash_amounts')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)