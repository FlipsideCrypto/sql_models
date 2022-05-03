{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'asgard_fund_yggdrasil_events']
) }}

SELECT
  block_timestamp,
  block_id,
  asset,
  tx_id,
  vault_key,
  asset_e8
FROM
  {{ ref('silver_thorchain__asgard_fund_yggdrasil_events') }}

{% if is_incremental() %}
WHERE block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
