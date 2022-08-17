{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'swap_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__swap_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, tx, chain, to_addr, from_addr, from_asset, from_e8, to_asset, to_e8, memo, pool, _direction
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
