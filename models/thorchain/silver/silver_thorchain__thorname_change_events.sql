{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'thorname_change_events']
) }}

SELECT
  *
FROM
  {{ ref(
    'thorchain_dbt__thorname_change_events'
  ) }}

{% if is_incremental() %}
WHERE
  __HEVO_loaded_at >= (
    SELECT
      MAX(__HEVO_loaded_at)
    FROM
      {{ this }}
  )
{% endif %}
