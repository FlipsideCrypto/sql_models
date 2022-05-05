{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'switch_events']
) }}

SELECT
  DISTINCT TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.burn_asset,
  e.burn_e8,
  e.to_addr AS to_address,
  e.from_addr AS from_address,
  e.tx
FROM
  {{ ref('thorchain_dbt__switch_events') }}
  e

qualify(ROW_NUMBER() over(PARTITION BY BLOCK_ID, BURN_ASSET, BLOCK_TIMESTAMP, TO_ADDRESS, FROM_ADDRESS
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
