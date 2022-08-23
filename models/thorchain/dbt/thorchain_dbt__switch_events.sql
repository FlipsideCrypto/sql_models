{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'switch_events']
) }}

SELECT
  COALESCE(
    tx,
    ''
  ) AS tx,
  COALESCE(
    from_addr,
    ''
  ) AS from_addr,
  COALESCE(
    to_addr,
    ''
  ) AS to_addr,
  COALESCE(
    burn_asset,
    ''
  ) AS burn_asset,
  burn_e8,
  mint_e8,
  block_timestamp,
  __HEVO__INGESTED_AT
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_switch_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
