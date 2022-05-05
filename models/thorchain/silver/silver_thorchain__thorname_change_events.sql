{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'thorname_change_events']
) }}

SELECT
  TO_TIMESTAMP(
    e.block_timestamp / 1000000000
  ) AS block_timestamp,
  bl.height AS block_id,
  e.owner,
  e.chain,
  e.address,
  e.expire,
  e.name,
  e.fund_amount_e8,
  e.registration_fee_e8
FROM
  {{ ref(
    'thorchain_dbt__thorname_change_events'
  ) }}
  e
qualify(ROW_NUMBER() over(PARTITION BY BLOCK_ID, OWNER, CHAIN, ADDRESS, BLOCK_TIMESTAMP
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
