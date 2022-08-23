{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'set_version_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__set_version_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, node_addr, block_timestamp, version
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
