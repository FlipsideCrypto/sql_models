{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'transfer_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.asset,
  e.amount_e8,
  e.from_addr AS from_address,
  e.to_addr AS to_address
FROM
  {{ ref('thorchain_dbt__transfer_events') }}
  e
qualify(ROW_NUMBER() over(PARTITION BY BLOCK_ID, ASSET, BLOCK_TIMESTAMP
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
