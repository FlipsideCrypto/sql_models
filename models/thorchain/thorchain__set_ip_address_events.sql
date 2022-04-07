{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'set_ip_address_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.ip_addr,
  e.node_addr AS node_address
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_set_ip_address_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
