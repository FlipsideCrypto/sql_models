{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'reward_claims', 'address_labels']
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
withdraw_msgs AS (
  SELECT
    tx_id,
    msg_index,
    msg_value :contract :: STRING AS claim_0_contract
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_value :execute_msg :withdraw IS NOT NULL
    AND msg_index = 0
    AND msg_value :contract :: STRING = 'terra1897an2xux840p9lrh6py3ryankc6mspw49xse3'
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
),
claim_msgs AS (
  SELECT
    tx_id,
    msg_index,
    msg_value :sender :: STRING AS sender,
    msg_value :contract :: STRING AS claim_1_contract
  FROM
    {{ ref('silver_terra__msgs') }}
  WHERE
    msg_value :execute_msg :claim_rewards IS NOT NULL
    AND msg_index = 1
    AND msg_value :contract :: STRING = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
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
),
withdraw AS (
  SELECT
    m.tx_id,
    claim_0_contract,
    event_attributes :"0_amount" / pow(
      10,
      6
    ) AS claim_0_amount,
    event_attributes :"1_contract_address" :: STRING AS claim_0_currency
  FROM
    {{ ref('silver_terra__msg_events') }}
    e
    JOIN withdraw_msgs m
    ON m.tx_id = e.tx_id
    AND m.msg_index = e.msg_index
  WHERE
    event_type = 'from_contract'
    AND tx_status = 'SUCCEEDED'
    AND event_attributes :"0_action" :: STRING = 'withdraw'

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
),
claim AS (
  SELECT
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    m.tx_id,
    sender,
    claim_1_contract,
    event_attributes :claim_amount / pow(
      10,
      6
    ) AS claim_1_amount,
    event_attributes :"2_contract_address" :: STRING AS claim_1_currency
  FROM
    {{ ref('silver_terra__msg_events') }}
    e
    JOIN claim_msgs m
    ON m.tx_id = e.tx_id
    AND m.msg_index = e.msg_index
  WHERE
    event_type = 'from_contract'
    AND tx_status = 'SUCCEEDED'
    AND event_attributes :"0_action" :: STRING = 'claim_rewards'

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
)
SELECT
  C.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  C.tx_id,
  sender,
  claim_0_amount,
  claim_0_amount * p0.price AS claim_0_amount_usd,
  claim_0_currency,
  claim_0_contract,
  l0.address AS claim_0_contract_label,
  claim_1_amount,
  claim_1_amount * p1.price AS claim_1_amount_usd,
  claim_1_currency,
  claim_1_contract,
  l1.address AS claim_1_contract_label
FROM
  claim C
  JOIN withdraw w
  ON C.tx_id = w.tx_id
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l0
  ON claim_0_contract = l0.address AND l0.blockchain = 'terra' AND l0.creator = 'flipside'
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l1
  ON claim_1_contract = l1.address AND l1.blockchain = 'terra' AND l1.creator = 'flipside'
  LEFT OUTER JOIN prices p0
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = p0.hour
  AND claim_0_currency = p0.currency
  LEFT OUTER JOIN prices p1
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = p1.hour
  AND claim_1_currency = p1.currency
