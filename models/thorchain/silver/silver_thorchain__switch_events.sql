{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'switch_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__switch_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY tx, from_addr, to_addr, burn_asset, burn_e8, mint_e8, block_timestamp
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
