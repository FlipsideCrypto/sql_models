{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', pool, asset_tx, asset_addr, rune_tx, rune_addr, pending_type, block_timestamp)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'pending_liquidity_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_pending_liquidity_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
