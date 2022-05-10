{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "concat_ws('-', 'blockchain','tx_id', 'asset', 'bond_type', 'pool_name', 'from_address', 'to_address')",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain', 'thorchain_bond_actions']
) }}

WITH block_prices AS (

  SELECT
    AVG(rune_usd) AS rune_usd,
    block_id
  FROM
    {{ ref('thorchain__prices') }}
  GROUP BY
    block_id
),
bond_events AS (
  SELECT
    *
  FROM
    {{ ref('thorchain__bond_events') }}
  WHERE
    TRUE
)
SELECT
  be.block_timestamp,
  be.block_id,
  tx_id,
  from_address,
  to_address AS to_address,
  asset,
  blockchain,
  bond_type,
  COALESCE(e8 / pow(10, 8), 0) AS asset_amount,
  COALESCE(
    rune_usd * asset_amount,
    0
  ) AS asset_usd
FROM
  bond_events be
  {% if is_incremental() %}
  WHERE be.block_timestamp >= getdate() - INTERVAL '5 days'
  {% endif %}
  LEFT JOIN block_prices p
  ON be.block_id = p.block_id
