{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'outbound_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.tx AS tx_id,
  e.asset,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.memo,
  e.asset_e8,
  e.chain AS blockchain,
  e.in_tx
FROM
  {{ ref('thorchain_dbt__outbound_events') }}
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