{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'gas_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__inactive_vault_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, add_asgard_addr, block_timestamp
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
