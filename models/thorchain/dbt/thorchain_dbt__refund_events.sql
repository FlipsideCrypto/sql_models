{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx, chain, from_addr, to_addr, asset, asset_2nd, asset_2nd_e8, memo, code, reason, block_timestamp)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'refund_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_refund_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
