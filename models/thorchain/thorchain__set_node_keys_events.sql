{{ 
  config(
    materialized='view', 
    tags=['snowflake', 'thorchain', 'set_node_keys_events']
  )
}}

SELECT
  to_timestamp(e.BLOCK_TIMESTAMP/1000000000) as block_timestamp,
  bl.height as block_id,
  e.NODE_ADDR as node_address,
  e.SECP256K1,
  e.ED25519,
  e.VALIDATOR_CONSENSUS
FROM {{source('thorchain_midgard', 'set_node_keys_events')}} e
INNER JOIN {{source('thorchain_midgard', 'block_log')}} bl ON bl.timestamp = e.BLOCK_TIMESTAMP
WHERE (e._FIVETRAN_DELETED IS NULL OR e._FIVETRAN_DELETED = False)