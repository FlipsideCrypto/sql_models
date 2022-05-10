{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'errata_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__errata_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY in_tx, asset, block_timestamp
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
