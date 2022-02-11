{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'redeem', 'address_labels']
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
),

redeem_events AS (
  SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  tx_status,
  msg_index,
  event_attributes
  FROM {{ ref('silver_terra__msg_events') }}
  WHERE tx_id in (select tx_id from terra.msgs WHERE msg_value:contract::string = 'terra1hzh9vpxhsk8253se0vv5jj6etdvxu3nv8z07zu')
  AND event_type = 'from_contract'
  AND event_attributes:redeem_amount is not null
  AND tx_status = 'SUCCEEDED'
)

SELECT DISTINCT
  e.blockchain,
  e.chain_id,
  e.block_id,
  e.block_timestamp,
  e.tx_id,
  msg_value :sender :: STRING AS sender,
  event_attributes:redeem_amount::FLOAT / pow(
    10,
    6
  ) AS amount,
  amount * price AS amount_usd,
  msg_value :contract :: STRING AS currency,
  COALESCE(msg_value :execute_msg :send :contract :: STRING, '') AS contract_address,
  COALESCE(l.address_name, '') AS contract_label
FROM redeem_events e
JOIN {{ ref('silver_terra__msgs') }} m
ON e.tx_id = m.tx_id
AND e.msg_index = m.msg_index
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON msg_value :execute_msg :send :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  LEFT OUTER JOIN prices r
  ON DATE_TRUNC(
    'hour',
    e.block_timestamp
  ) = HOUR
  AND msg_value :contract :: STRING = r.currency
WHERE e.tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND e.block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
