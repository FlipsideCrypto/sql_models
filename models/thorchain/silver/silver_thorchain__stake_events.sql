{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'stake_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__stake_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY pool, rune_addr, asset_tx, asset_addr, block_timestamp
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
