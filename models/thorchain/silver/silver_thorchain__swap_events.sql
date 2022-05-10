{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'swap_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__swap_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY tx, to_addr, from_addr
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
