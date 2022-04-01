{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'set_node_keys_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.node_addr AS node_address,
  e.secp256k1,
  e.ed25519,
  e.validator_consensus
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_set_node_keys_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
