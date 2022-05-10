{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'message_events']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__message_events') }}

{% if is_incremental() %}
WHERE __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
