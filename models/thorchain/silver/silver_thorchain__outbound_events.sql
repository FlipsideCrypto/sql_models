{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'outbound_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__outbound_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, tx, chain, from_addr, to_addr, asset, memo, in_tx, block_timestamp
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
