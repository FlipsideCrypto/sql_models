{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'refund_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__refund_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY block_id, code, block_timestamp, asset, asset_2nd, to_address, from_address
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
