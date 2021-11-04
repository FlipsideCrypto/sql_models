{{ config(
  materialized = 'incremental',
  unique_key = 'date|| address|| currency || balance_type',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'gold', 'gold_flow', 'gold__flow_daily_balances_dedupped']
) }}

SELECT
  date,
  address,
  balance,
  currency,
  balance_type
FROM
  {{ source(
      'flow',
      'daily_balances'
    ) }}
WHERE
  1 = 1

{% if is_incremental() %}
AND date :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(date :: DATE))
  FROM
    {{ this }} AS flow_daily_balances_dedupped
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY date, address, currency, balance_type
ORDER BY
  date DESC)) = 1
