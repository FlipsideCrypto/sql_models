{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', to_addr, from_asset, to_asset, memo, pool, swap_slip_bp, _direction, block_timestamp)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'swap_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_swap_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
