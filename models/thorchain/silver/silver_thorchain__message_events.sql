{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'message_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.action,
  e.from_addr AS from_address
FROM
  {{ ref('thorchain_dbt__message_events') }}

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
