{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'bonds', 'anchor_bonds', 'address_labels']
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
    ) AS bonded_amount,
    bonded_amount * price AS bonded_amount_usd,
    msg_value :coins [0] :denom :: STRING AS bonded_currency,
    msg_value :execute_msg :bond :validator :: STRING AS validator,
    msg_value :contract :: STRING AS contract_address,
    l.address_name AS contract_label
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
    msg_value :execute_msg :bond IS NOT NULL
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
    event_attributes :"exchange_rate" / pow(
      10,
      6
    ) AS minted_amount,
    minted_amount * price AS minted_amount_usd,
    event_attributes :"denom" :: STRING AS minted_currency
  FROM
    {{ ref('silver_terra__msg_events') }}
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = o.hour
    AND event_attributes :"denom" :: STRING = o.currency
  WHERE
    tx_id IN(
      SELECT
        tx_id
      FROM
        msgs
    )
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
SELECT DISTINCT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender,
  bonded_amount,
  bonded_amount_usd,
  bonded_currency,
  msg_index,
  validator,
  COALESCE(contract_address, '') AS contract_address,
  COALESCE(contract_label, '') AS contract_label
FROM
  msgs m
  JOIN events e
  ON m.tx_id = e.tx_id
