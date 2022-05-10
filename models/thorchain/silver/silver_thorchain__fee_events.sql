{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'fee_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__fee_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY tx, asset, pool_deduct, block_timestamp
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
