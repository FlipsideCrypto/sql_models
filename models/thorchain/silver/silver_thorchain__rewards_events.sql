{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'rewards_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__rewards_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, block_timestamp
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
WHERE
  __HEVO_loaded_at >= (
    SELECT
      MAX(__HEVO_loaded_at)
    FROM
      {{ this }}
  )
{% endif %}
