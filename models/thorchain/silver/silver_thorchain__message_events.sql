{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'message_events']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__message_events') }}

qualify(ROW_NUMBER() over(PARTITION BY ACTION, FROM_ADDRESS, BLOCK_ID, BLOCK_TIMESTAMP
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
WHERE __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
