{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'deposits']
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
msgs AS (
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_index,
    msg_value :sender :: STRING AS sender,
    msg_value :coins [0] :amount / pow(
      10,
      6
    ) AS deposit_amount,
    deposit_amount * price AS deposit_amount_usd,
    msg_value :coins [0] :denom :: STRING AS deposit_currency,
    msg_value :contract :: STRING AS contract_address,
    l.address AS contract_label
  FROM
    {{ ref('silver_terra__msgs') }}
    m
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = o.hour
    AND msg_value :coins [0] :denom :: STRING = o.currency
  WHERE
    msg_value :execute_msg :deposit_stable IS NOT NULL
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
events AS (
  SELECT
    tx_id,
    msg_index,
    event_attributes :mint_amount / pow(
      10,
      6
    ) AS mint_amount,
    mint_amount * price AS mint_amount_usd,
    event_attributes :"1_contract_address" :: STRING AS mint_currency
  FROM
    {{ ref('silver_terra__msg_events') }}
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = o.hour
    AND event_attributes :"1_contract_address" :: STRING = o.currency
  WHERE
    event_type = 'from_contract'
    AND tx_id IN(
      SELECT
        tx_id
      FROM
        msgs
    )
    AND event_attributes :"0_action" :: STRING = 'deposit_stable'
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
)
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender,
  deposit_amount,
  deposit_amount_usd,
  deposit_currency,
  mint_amount,
  mint_amount_usd,
  mint_currency,
  contract_address,
  contract_label
FROM
  msgs m
  JOIN events e
  ON m.tx_id = e.tx_id
