{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', pool, asset, asset_e8, block_timestamp)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'slash_amounts']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_slash_amounts'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
