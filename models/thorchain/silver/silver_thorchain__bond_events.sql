{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'bond_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__bond_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY tx, from_addr, asset_e8, bond_type, e8, block_timestamp, to_addr, chain, asset, memo
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1

{% if is_incremental() %}
WHERE
  __HEVO_loaded_at >= (
    SELECT
      MAX(__HEVO_loaded_at)
    FROM
      {{ this }}
  )
{% endif %}
