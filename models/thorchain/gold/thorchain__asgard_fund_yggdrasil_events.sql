{{ config(
  materialized = 'view',
  tags = ['snowflake', 'thorchain', 'asgard_fund_yggdrasil_events']
) }}

SELECT
  TO_TIMESTAMP(
    d.block_timestamp / 1000000000
  ) AS block_timestamp,
  asset,
  tx AS tx_id,
  vault_key,
  asset_e8
FROM
  {{ ref('silver_thorchain__asgard_fund_yggdrasil_events') }}

{% if is_incremental() %}
WHERE block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
