{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx, chain, from_addr, to_addr, asset, pool, block_timestamp, memo)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'add_events']
) }}


SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_add_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
