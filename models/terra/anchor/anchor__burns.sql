{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'burns', 'address_labels']
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
  msg_value :execute_msg :send :amount / pow(
    10,
    6
  ) AS amount,
  amount * price AS amount_usd,
  msg_value :contract :: STRING AS currency,
  msg_value :execute_msg :send :contract :: STRING AS contract_address,
  COALESCE(l.address_name, '') AS contract_label
FROM
  {{ ref('silver_terra__msgs') }}
  m
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = o.hour
  AND msg_value :contract :: STRING = o.currency
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON msg_value :execute_msg :send :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
WHERE
  msg_value :execute_msg :send :msg :unbond IS NOT NULL
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
