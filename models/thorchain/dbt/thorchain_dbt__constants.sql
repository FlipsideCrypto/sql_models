{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'constants']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_constants'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
