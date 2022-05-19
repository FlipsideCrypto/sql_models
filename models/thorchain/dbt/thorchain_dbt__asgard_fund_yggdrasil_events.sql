{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx, asset, block_timestamp, vault_key)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'thorchain_dbt', 'asgard_fund_yggdrasil_events']
) }}

SELECT
  *
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_asgard_fund_yggdrasil_events'
  ) }}

{% if is_incremental() %}
AND __HEVO_loaded_at >= (
  SELECT
    MAX(__HEVO_loaded_at)
  FROM
    {{ this }}
)
{% endif %}
