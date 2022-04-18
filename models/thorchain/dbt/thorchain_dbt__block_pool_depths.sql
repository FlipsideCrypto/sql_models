{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'block_pool_depths']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_block_pool_depths'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
