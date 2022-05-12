{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'switch_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__switch_events') }}

{% if is_incremental() %}
WHERE
  __HEVO_loaded_at >= (
    SELECT
      MAX(__HEVO_loaded_at)
    FROM
      {{ this }}
  )
{% endif %}
