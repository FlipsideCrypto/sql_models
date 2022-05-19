{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', asset, rune_amt, rune_add, asset_amt, asset_add, reason, block_timestamp)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'pool_balance_change_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_pool_balance_change_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
