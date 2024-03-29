{{ config(
  materialized = 'view',
  tags = ['snowflake', 'silver_thorchain', 'validator_request_leave_events']
) }}

SELECT
  *
FROM
  {{ ref('thorchain_dbt__validator_request_leave_events') }}
  e qualify(ROW_NUMBER() over(PARTITION BY event_id, tx, block_timestamp, from_addr, node_addr
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
