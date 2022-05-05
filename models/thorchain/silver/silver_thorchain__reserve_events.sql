{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'reserve_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__reserve_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id, blockchain, address, from_address, to_address, asset
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
