{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'slash_points']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_slash_points'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
