{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "CONCAT_WS('-', block_id, pool_name)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain', 'thorchain_pool_block_balances']
) }}

SELECT
  DISTINCT bpd.block_timestamp,
  bpd.block_id,
  bpd.pool_name,
  COALESCE(rune_e8 / pow(10, 8), 0) AS rune_amount,
  COALESCE(rune_e8 / pow(10, 8), 0) * rune_usd AS rune_amount_usd,
  COALESCE(asset_e8 / pow(10, 8), 0) AS asset_amount,
  COALESCE(asset_e8 / pow(10, 8), 0) * asset_usd AS asset_amount_usd,
  COALESCE(synth_e8 / pow(10, 8), 0) AS synth_amount,
  COALESCE(synth_e8 / pow(10, 8), 0) * asset_usd AS synth_amount_usd
FROM
  {{ ref('thorchain__block_pool_depths') }}
  bpd
  LEFT JOIN {{ ref('thorchain__prices') }}
  p
  ON bpd.block_id = p.block_id
  AND bpd.pool_name = p.pool_name
