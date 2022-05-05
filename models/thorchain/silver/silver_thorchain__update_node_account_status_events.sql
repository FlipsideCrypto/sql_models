{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'update_node_account_status_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.node_addr AS node_address,
  e."CURRENT" AS current_status,
  e.former AS former_status
FROM
  {{ ref('thorchain_dbt__update_node_account_status_events') }}
  e
qualify(ROW_NUMBER() over(PARTITION BY BLOCK_ID, NODE_ADDRESS, BLOCK_TIMESTAMP, FORMER_STATUS
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