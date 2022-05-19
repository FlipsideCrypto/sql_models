{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx, chain, from_addr, to_addr, asset, memo, pool, stake_units, basis_points, asymmetry, block_timestamp)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'unstake_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_unstake_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
