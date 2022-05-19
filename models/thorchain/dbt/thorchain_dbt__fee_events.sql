{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx, asset_e8, asset, pool_deduct, block_timestamp)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'fee_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_fee_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
