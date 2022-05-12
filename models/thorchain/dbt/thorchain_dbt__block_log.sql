{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'block_log']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_block_log'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
