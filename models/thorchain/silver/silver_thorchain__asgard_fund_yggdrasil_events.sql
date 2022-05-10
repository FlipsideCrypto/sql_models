{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'asgard_fund_yggdrasil_events']
) }}

SELECT *
FROM
  {{ ref('thorchain_dbt__asgard_fund_yggdrasil_events') }}

qualify(ROW_NUMBER() over(PARTITION BY TX, ASSET, ASSET_E8, VAULT_KEY, BLOCK_TIMESTAMP
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