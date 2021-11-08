{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'repay']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price
  FROM
    {{ ref('terra__oracle_prices') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
GROUP BY
  1,
  2,
  3
)
SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value :sender :: STRING AS sender,
  msg_value :coins [0] :amount / pow(
    10,
    6
  ) AS amount,
  amount * price AS amount_usd,
  msg_value :coins [0] :denom :: STRING AS currency,
  msg_value :contract :: STRING AS contract_address,
  l.address AS contract_label
FROM
  {{ ref('silver_terra__msgs') }}
  m
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON msg_value :contract :: STRING = l.address
  LEFT OUTER JOIN prices r
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = HOUR
  AND msg_value :coins [0] :denom :: STRING = r.currency
WHERE
  msg_value :execute_msg :repay_stable IS NOT NULL
  AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
