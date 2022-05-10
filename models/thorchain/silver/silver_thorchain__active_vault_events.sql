{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'active_vault_events']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__active_vault_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY add_asgard_addr, block_timestamp
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
WHERE
  __HEVO__LOADED_AT >= (
    SELECT
      MAX(__HEVO__LOADED_AT)
    FROM
      {{ this }}
  )
{% endif %}
