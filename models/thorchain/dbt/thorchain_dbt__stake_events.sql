{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', pool, asset_tx, asset_chain, asset_addr, asset_e8, stake_units, rune_tx, rune_addr, rune_e8, block_timestamp)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'stake_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_stake_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
