{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'new_node_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.node_addr AS node_address
FROM
  {{ ref('thorchain_dbt__new_node_events') }}
  e
  INNER JOIN {{ ref('thorchain_dbt__block_log') }}
  bl
  ON bl.timestamp = e.block_timestamp

qualify(ROW_NUMBER() over(PARTITION BY ASSET, TX_COUNT, BLOCK_TIMESTAMP
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