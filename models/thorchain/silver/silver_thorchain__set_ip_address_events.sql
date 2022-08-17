{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'set_ip_address_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__set_ip_address_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, ip_addr, block_timestamp, node_addr
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
