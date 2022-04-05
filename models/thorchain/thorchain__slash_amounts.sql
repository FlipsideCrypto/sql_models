{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'slash_amounts']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.asset_e8,
  e.pool AS pool_name,
  e.asset
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_slash_amounts'
  ) }}
  e
  INNER JOIN {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}
  bl
  ON bl.timestamp = e.block_timestamp
