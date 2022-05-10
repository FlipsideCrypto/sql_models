{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'add_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__add_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY tx, chain, from_addr, to_addr, asset, memo, pool, block_timestamp
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
