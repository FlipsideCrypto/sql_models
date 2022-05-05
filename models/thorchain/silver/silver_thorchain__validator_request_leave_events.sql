{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'validator_request_leave_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.from_addr AS from_address,
  e.node_addr AS node_address
FROM
  {{ ref('thorchain_dbt__validator_request_leave_events') }}
  e
qualify(ROW_NUMBER() over(PARTITION BY BLOCK_ID, TX_ID, BLOCK_TIMESTAMP, FROM_ADDRESS, NODE_ADDRESS
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
