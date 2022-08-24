{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'bond_events']
) }}

SELECT
  tx,
  COALESCE(
    chain,
    ''
  ) AS chain,
  COALESCE(
    from_addr,
    ''
  ) AS from_addr,
  COALESCE(
    to_addr,
    ''
  ) AS to_addr,
  COALESCE(
    asset,
    ''
  ) AS asset,
  asset_e8,
  COALESCE(
    memo,
    ''
  ) AS memo,
  COALESCE(
    bond_type,
    ''
  ) AS bond_type,
  e8,
  block_timestamp,
  __HEVO__INGESTED_AT
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_bond_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
