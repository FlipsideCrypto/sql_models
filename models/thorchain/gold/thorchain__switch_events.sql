{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'switch_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.burn_asset,
  e.burn_e8,
  e.mint_e8,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.tx
FROM
  {{ ref('silver_thorchain__switch_events') }}
  e
  INNER JOIN {{ ref('silver_thorchain__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
