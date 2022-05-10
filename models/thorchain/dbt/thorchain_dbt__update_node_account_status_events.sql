{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'update_node_account_status_events']
) }}

SELECT
  node_addr,
  "CURRENT" AS current_flag,
  former,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_update_node_account_status_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
