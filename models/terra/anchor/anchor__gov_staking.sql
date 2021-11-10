{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'anchor_gov']
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
stake_msgs AS (
  SELECT
    t.blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :execute_msg :send :amount / pow(
      10,
      6
    ) AS amount,
    amount * o.price AS amount_usd,
    msg_value :contract :: STRING AS currency,
    msg_value :execute_msg :send :contract :: STRING AS contract_address,
    l.address AS contract_label
  FROM
    {{ ref('silver_terra__msgs') }}
    t
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = o.hour
    AND msg_value :contract :: STRING = o.currency
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :execute_msg :send :contract :: STRING = l.address
  WHERE
    msg_value :execute_msg :send :msg :stake_voting_tokens IS NOT NULL
    AND msg_value :execute_msg :send :contract :: STRING = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5'
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
stake_events AS (
  SELECT
    tx_id,
    event_attributes :share AS shares
  FROM
    {{ ref('silver_terra__msg_events') }}
  WHERE
    tx_id IN(
      SELECT
        DISTINCT tx_id
      FROM
        stake_msgs
    )
    AND event_type = 'from_contract'

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
) -- Staking
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  'stake' AS event_type,
  sender,
  amount,
  amount_usd,
  currency,
  shares,
  contract_address,
  contract_label
FROM
  stake_msgs m
  JOIN stake_events e
  ON m.tx_id = e.tx_id
UNION
  -- Unstaking
SELECT
  t.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  'unstake' AS event_type,
  msg_value :sender :: STRING AS sender,
  msg_value :execute_msg :withdraw_voting_tokens :amount / pow(
    10,
    6
  ) AS amount,
  amount * o.price AS amount_usd,
  'terra14z56l0fp2lsf86zy3hty2z47ezkhnthtr9yq76' AS currency,
  NULL AS shares,
  msg_value :contract :: STRING AS contract_address,
  l.address AS contract_label
FROM
  {{ ref('silver_terra__msgs') }}
  t
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    t.block_timestamp
  ) = o.hour
  AND 'terra14z56l0fp2lsf86zy3hty2z47ezkhnthtr9yq76' = o.currency
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON msg_value :contract :: STRING = l.address
WHERE
  msg_value :execute_msg :withdraw_voting_tokens IS NOT NULL
  AND msg_value :contract :: STRING = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5'
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
