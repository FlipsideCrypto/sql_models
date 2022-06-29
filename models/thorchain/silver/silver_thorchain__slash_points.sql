{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'slash_points']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__slash_points') }}

{% if is_incremental() %}
WHERE
  __HEVO_loaded_at >= (
    SELECT
      MAX(__HEVO_loaded_at)
    FROM
      {{ this }}
  )
{% endif %}
