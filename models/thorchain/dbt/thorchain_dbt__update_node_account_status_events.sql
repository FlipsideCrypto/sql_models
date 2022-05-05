{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain_dbt', 'update_node_account_status_events']
) }}

SELECT 
  TRY_CAST("NODE_ADDR" AS VARCHAR) AS node_addr,
  TRY_CAST("CURRENT" AS VARCHAR) AS "current",
  FORMER,
  BLOCK_TIMESTAMP,
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