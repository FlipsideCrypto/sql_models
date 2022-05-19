{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', from_addr, to_addr, asset, block_timestamp)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'transfer_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_transfer_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
