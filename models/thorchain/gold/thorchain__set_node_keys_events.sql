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
  {{ ref('silver_thorchain__set_node_keys_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
{% if is_incremental() %}
WHERE e.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}