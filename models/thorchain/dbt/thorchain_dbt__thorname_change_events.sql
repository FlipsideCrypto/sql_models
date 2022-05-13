{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'thorname_change_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_thorname_change_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
