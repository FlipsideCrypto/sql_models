{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "CONCAT_WS('-', tx_id, block_id, to_asset, from_asset, block_timestamp, native_to_address, from_address, pool_name, to_pool_address)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain', 'swaps']
) }}

WITH swaps AS (

  SELECT
    *,
    COUNT(*) over (
      PARTITION BY tx_id
    ) AS n_tx,
    RANK() over (
      PARTITION BY tx_id
      ORDER BY
        liq_fee_e8 ASC
    ) AS rank_liq_fee
  FROM
    {{ ref('thorchain__swap_events') }}
)
SELECT
  se.block_timestamp,
  se.block_id,
  tx_id,
  blockchain,
  se.pool_name,
  from_address,
  CASE
    WHEN n_tx > 1
    AND rank_liq_fee = 1 
    AND SPLIT(
      memo,
      ':'
    ) [4] :: STRING IS NOT NULL
    THEN SPLIT(
      memo,
      ':'
    ) [4] :: STRING
    ELSE SPLIT(
      memo,
      ':'
    ) [2] :: STRING
  END AS native_to_address,
  to_address AS to_pool_address,
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

{% if is_incremental() %}
WHERE
  se.block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
