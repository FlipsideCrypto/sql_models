{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'add_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_add_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
