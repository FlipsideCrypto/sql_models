{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'errata_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.asset_e8,
  e.rune_e8,
  e.in_tx,
  e.asset
FROM
  {{ ref('thorchain_dbt__errata_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp
