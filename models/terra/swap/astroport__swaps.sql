{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, msg_index, tx_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'astroport', 'swap', 'address_labels']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC('hour',block_timestamp) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price
  FROM 
    {{ ref('terra__oracle_prices') }}
  WHERE 1=1
  {% if is_incremental() %}
  AND block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

GROUP BY
  1,
  2,
  3
),

source_event_actions AS (
  SELECT
    a.blockchain,
    a.chain_id,
    a.block_id,
    a.msg_index,
    action_index AS tx_index,
    a.block_timestamp,
    a.tx_id,
    coalesce(msg_value :sender :: STRING, action_log :sender ::STRING) AS sender,
    action_log :offer_amount:: numeric / pow(10,6) AS offer_amount,
    action_log :offer_asset::STRING AS offer_currency,
    action_log :return_amount:: numeric / pow(10,6) AS return_amount,
    action_log :ask_asset::STRING AS return_currency,
    action_contract_address::STRING AS contract_address
  FROM {{ ref('silver_terra__event_actions') }} a
   LEFT JOIN {{ ref('silver_terra__msgs') }} m
   ON a.tx_id = m.tx_id AND a.msg_index = m.msg_index
  WHERE action_method = 'swap'
  AND action_log :maker_fee_amount IS NOT NULL
  {% if is_incremental() %}
  AND a.block_timestamp :: DATE >= (SELECT MAX(block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}
),

source_address_labels AS (
  SELECT 
    * 
  FROM {{ ref('silver_crosschain__address_labels') }}
),

astro_pairs AS (
  SELECT 
    event_attributes :pair_contract_addr::STRING AS contract_address
  FROM {{ ref('silver_terra__msg_events') }}
  WHERE tx_id IN (SELECT
                    tx_id
                  FROM {{ ref('silver_terra__msgs') }}
                  WHERE msg_value :execute_msg :create_pair IS NOT NULL
  				  AND msg_value :contract = 'terra1fnywlw4edny3vw44x04xd67uzkdqluymgreu7g'
                 )
  AND event_type = 'from_contract'
)

SELECT DISTINCT
      e.blockchain,
      chain_id,
      block_id,
      msg_index,
      tx_index,
      e.block_timestamp,
      e.tx_id,
      sender,
      offer_amount,
      offer_amount * o.price AS offer_amount_usd,
      offer_currency,
      return_amount,
      return_amount * r.price AS return_amount_usd,
      return_currency,
      e.contract_address AS pool_address,
      coalesce(l.address_name, d.symbol) AS pool_name
    FROM
      source_event_actions e

    LEFT OUTER JOIN prices o
      ON DATE_TRUNC('hour',e.block_timestamp) = o.hour
      AND e.offer_currency = o.currency
      
    LEFT OUTER JOIN prices r
      ON DATE_TRUNC('hour',e.block_timestamp) = r.hour
      AND e.return_currency = r.currency

    LEFT OUTER JOIN source_address_labels l
      ON e.contract_address = l.address 
      AND l.blockchain = 'terra' 
      AND l.creator = 'flipside'

    LEFT OUTER JOIN {{ ref('silver_terra__dex_contracts') }} d
    ON e.contract_address = d.contract_address
    WHERE offer_amount IS NOT NULL
    AND offer_currency IS NOT NULL
