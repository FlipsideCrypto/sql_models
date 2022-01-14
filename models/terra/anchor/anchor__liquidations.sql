{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'liquidations', 'address_labels']
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
    msg_value :execute_msg :liquidate_collateral :borrower :: STRING AS borrower,
    msg_value :contract :: STRING AS contract_address,
    l.address_name AS contract_label
  FROM
    {{ ref('silver_terra__msgs') }}
    m
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  WHERE
    msg_value :contract :: STRING = 'terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8'
    AND msg_value :execute_msg :liquidate_collateral IS NOT NULL
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
events_first_token AS (
  SELECT
    tx_id,
    COALESCE(event_attributes :liquidator :: STRING, event_attributes :"0_liquidator" :: STRING) AS liquidator,
    COALESCE(event_attributes :collateral_amount / pow(
      10,
      6
    ), event_attributes :"0_collateral_amount" / pow(
      10,
      6
    )) AS liquidated_amount,
    liquidated_amount * l.price AS liquidated_amount_usd,
    COALESCE(event_attributes :collateral_token :: STRING, event_attributes :"0_collateral_token" :: STRING) AS liquidated_currency,
    CASE WHEN 
      event_attributes :"2_repay_amount" IS NULL 
    THEN
      event_attributes :"1_repay_amount" / pow(
        10,
        6
      ) 
    ELSE
      event_attributes :"0_repay_amount" / pow(
        10,
        6
      )
    END AS repay_amount,
    event_attributes :"1_borrower" :: STRING AS borrower,
    repay_amount * r.price AS repay_amount_usd,
    COALESCE(event_attributes :stable_denom :: STRING, event_attributes :"0_stable_denom" :: STRING) AS repay_currency,
    COALESCE(event_attributes :bid_fee / pow(
      10,
      6
    ), event_attributes :"0_bid_fee" / pow(
      10,
      6
    )) AS bid_fee
  FROM
    {{ ref('silver_terra__msg_events') }}
    LEFT OUTER JOIN prices l
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = l.hour
    AND COALESCE(event_attributes :collateral_token :: STRING, event_attributes :"0_collateral_token" :: STRING) = l.currency
    LEFT OUTER JOIN prices r
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = r.hour
    AND COALESCE(event_attributes :stable_denom :: STRING, event_attributes :"0_stable_denom" :: STRING) = r.currency
  WHERE
    event_type = 'from_contract'
    AND tx_id IN(
      SELECT
        tx_id
      FROM
        msgs
    )
    AND tx_status = 'SUCCEEDED'
    AND event_attributes :"0_action" :: STRING = 'liquidate_collateral'

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
events_second_tokens AS (
  SELECT
    tx_id,
    COALESCE(event_attributes :liquidator :: STRING, event_attributes :"1_liquidator" :: STRING) AS liquidator,
    COALESCE(event_attributes :collateral_amount / pow(
      10,
      6
    ), event_attributes :"1_collateral_amount" / pow(
      10,
      6
    )) AS liquidated_amount,
    liquidated_amount * l.price AS liquidated_amount_usd,
    COALESCE(event_attributes :collateral_token :: STRING, event_attributes :"1_collateral_token" :: STRING) AS liquidated_currency,
    event_attributes :"1_repay_amount" / pow(
      10,
      6
    ) AS repay_amount,
    event_attributes :"1_borrower" :: STRING AS borrower,
    repay_amount * r.price AS repay_amount_usd,
    COALESCE(event_attributes :stable_denom :: STRING, event_attributes :"1_stable_denom" :: STRING) AS repay_currency,
    COALESCE(event_attributes :bid_fee / pow(
      10,
      6
    ), event_attributes :"1_bid_fee" / pow(
      10,
      6
    )) AS bid_fee
  FROM
    {{ ref('silver_terra__msg_events') }}
    LEFT OUTER JOIN prices l
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = l.hour
    AND COALESCE(event_attributes :collateral_token :: STRING, event_attributes :"0_collateral_token" :: STRING) = l.currency
    LEFT OUTER JOIN prices r
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = r.hour
    AND COALESCE(event_attributes :stable_denom :: STRING, event_attributes :"0_stable_denom" :: STRING) = r.currency
  WHERE
    event_type = 'from_contract'
    AND tx_id IN(
      SELECT
        tx_id
      FROM
        msgs
    )
    AND tx_status = 'SUCCEEDED'
    AND event_attributes :"0_action" :: STRING = 'liquidate_collateral'
    AND event_attributes :"2_repay_amount" IS NOT NULL 

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
  SELECT * FROM events_first_token
  UNION ALL 
  SELECT * FROM events_second_tokens
),
single_payment_tbl AS (
  SELECT DISTINCT
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    m.tx_id,
    bid_fee,
    m.borrower,
    liquidator,
    liquidated_amount,
    liquidated_amount_usd,
    liquidated_currency,
    repay_amount,
    repay_amount_usd,
    repay_currency,
    contract_address,
    contract_label
  FROM
    msgs m
    JOIN events e
    ON m.tx_id = e.tx_id
    AND m.borrower = e.borrower 
),
multiple_repay_tbl_events_raw AS (
  SELECT 
    * 
  FROM {{ ref('silver_terra__msg_events') }}
  WHERE
    event_type = 'from_contract'
    AND tx_status = 'SUCCEEDED'
    AND event_attributes :"0_action" :: STRING = 'liquidate_collateral'
    AND event_attributes :"3_repay_amount" IS NOT NULL
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
multiple_repay_tbl_raw AS (
  SELECT 
    * 
  FROM multiple_repay_tbl_events_raw
  , lateral flatten ( input => event_attributes )
  WHERE key LIKE '%_repay_amount' AND split(key, '_')[0]%2 = 1
),
multiple_repay_tbl AS (
  SELECT 
    blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    COALESCE(this :bid_fee / pow(
        10,
        6
      ), this :"0_bid_fee" / pow(
        10,
        6
      )) AS bid_fee,
  
    this :"0_borrower" :: STRING AS borrower,
    COALESCE(this :liquidator :: STRING, this :"0_liquidator" :: STRING) AS liquidator,
    COALESCE(this :collateral_amount / pow(
        10,
        6
      ), this :"0_collateral_amount" / pow(
        10,
        6
      )) AS liquidated_amount,
    liquidated_amount * l.price AS liquidated_amount_usd,
    COALESCE(this :collateral_token :: STRING, this :"0_collateral_token" :: STRING) AS liquidated_currency,
    value / pow(
        10,
        6
      ) AS repay_amount,
    repay_amount * r.price AS repay_amount_usd,
    COALESCE(this :stable_denom :: STRING, this :"0_stable_denom" :: STRING) AS repay_currency,
    'terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8' AS contract_address,
    'Overseer' AS contract_label
  FROM multiple_repay_tbl_raw
  LEFT OUTER JOIN prices l
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = l.hour
  AND COALESCE(event_attributes :collateral_token :: STRING, event_attributes :"0_collateral_token" :: STRING) = l.currency
  LEFT OUTER JOIN prices r
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = r.hour
  AND COALESCE(event_attributes :stable_denom :: STRING, event_attributes :"0_stable_denom" :: STRING) = r.currency
)

SELECT * FROM single_payment_tbl
UNION ALL
SELECT * FROM multiple_repay_tbl


