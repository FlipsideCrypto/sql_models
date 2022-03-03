{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "CONCAT_WS('-', tx_id, to_asset, from_asset)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain', 'thorchain_swaps']
) }}

WITH swaps AS (

  SELECT
    *
  FROM
    {{ ref('thorchain__swap_events') }}
  WHERE
    TRUE

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '2 days'
{% endif %}
)
SELECT
  se.block_timestamp,
  se.block_id,
  tx_id,
  blockchain,
  se.pool_name,
  from_address,
  to_address,
  from_asset,
  to_asset,
  COALESCE(from_e8 / pow(10, 8), 0) AS from_amount,
  COALESCE(to_e8 / pow(10, 8), 0) AS to_amount,
  COALESCE(to_e8_min / pow(10, 8), 0) AS min_to_amount,
  CASE
    WHEN from_asset = 'THOR.RUNE' THEN COALESCE(from_e8 * rune_usd / pow(10, 8), 0)
    ELSE COALESCE(from_e8 * asset_usd / pow(10, 8), 0)
  END AS from_amount_usd,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN COALESCE(to_e8 * rune_usd / pow(10, 8), 0)
    ELSE COALESCE(to_e8 * asset_usd / pow(10, 8), 0)
  END AS to_amount_usd,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN COALESCE(to_e8_min * rune_usd / pow(10, 8), 0)
    ELSE COALESCE(to_e8_min * asset_usd / pow(10, 8), 0)
  END AS to_amount_min_usd,
  swap_slip_bp,
  COALESCE(liq_fee_in_rune_e8 / pow(10, 8), 0) AS liq_fee_rune,
  COALESCE(liq_fee_in_rune_e8 / pow(10, 8) * rune_usd, 0) AS liq_fee_rune_usd,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN COALESCE(liq_fee_e8 / pow(10, 8), 0)
    ELSE COALESCE(liq_fee_e8 / pow(10, 8), 0)
  END AS liq_fee_asset,
  CASE
    WHEN to_asset = 'THOR.RUNE' THEN COALESCE(liq_fee_e8 * rune_usd / pow(10, 8), 0)
    ELSE COALESCE(liq_fee_e8 * asset_usd / pow(10, 8), 0)
  END AS liq_fee_asset_usd
FROM
  swaps se
  LEFT JOIN {{ ref('thorchain__prices') }}
  p
  ON se.block_id = p.block_id
  AND se.pool_name = p.pool_name
