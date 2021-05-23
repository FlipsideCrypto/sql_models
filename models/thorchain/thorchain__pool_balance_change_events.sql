{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'pool_balance_change_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.REASON,
  e.ASSET,
  e.RUNE_AMT as rune_amount,
  e.ASSET_ADD,
  e.ASSET_AMT	as asset_amount,
  e.RUNE_ADD
FROM {{source('thorchain_midgard', 'pool_balance_change_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)