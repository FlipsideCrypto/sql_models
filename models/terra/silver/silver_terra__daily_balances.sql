{{ config(
  materialized = 'incremental',
  unique_key = "_UK",
  cluster_by = ['DATE'],
  tags = ['snowflake', 'silver_terra', 'silver_terra__daily_balances']
) }}

SELECT
  concat_ws(
    '-',
    block_timestamp :: DATE,
    address,
    currency,
    balance_type
  ) AS _UK,
  address,
  currency,
  block_timestamp :: DATE AS DATE,
  balance_type,
  'terra' AS blockchain,
  balance
FROM
  {{ source(
    'shared',
    'terra_balances'
  ) }}
WHERE
  balance > 0

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '3 DAYS'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY address, currency, block_timestamp :: DATE, balance_type
ORDER BY
  block_timestamp DESC)) = 1
