{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'unstake_events']
) }}

SELECT
  *
FROM
  {{ ref(
    'thorchain_dbt__unstake_events'
  ) }}
  e qualify(ROW_NUMBER() over(PARTITION BY event_id, tx, chain, memo, stake_units, basis_points, block_timestamp, pool, asset, from_addr, to_addr
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
