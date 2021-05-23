{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'asgard_fund_yggdrasil_events']
  )
}}

SELECT
    to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
    bl.height as block_id,
    e.ASSET,
    e.TX as tx_id,
    e.VAULT_KEY,
    e.ASSET_E8
FROM {{source('thorchain_midgard', 'asgard_fund_yggdrasil_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP