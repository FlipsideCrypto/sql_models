{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'switch_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__switch_events') }}
  e qualify(ROW_NUMBER() over(PARTITION BY tx, burn_asset, block_timestamp, to_addr, from_addr
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
