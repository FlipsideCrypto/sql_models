{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'set_version_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.node_addr AS node_address,
  e.version
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_set_version_events'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
