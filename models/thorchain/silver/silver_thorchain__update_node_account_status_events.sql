{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'update_node_account_status_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__update_node_account_status_events') }}
  e qualify(ROW_NUMBER() over(PARTITION BY node_addr, block_timestamp, former, current_flag
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
