{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'bond_events']
) }}

SELECT 
  *
FROM
  {{ ref('thorchain_dbt__bond_events') }}

qualify(ROW_NUMBER() over(PARTITION BY TX, CHAIN, FROM_ADDR, TO_ADDR, ASSET, BOND_TYPE
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}