{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'anchor', 'repay', 'address_labels']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC('hour', block_timestamp) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price
  FROM {{ ref('terra__oracle_prices') }}
  WHERE
    1 = 1

  {% if is_incremental() %}
    AND block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
  {% endif %}

  GROUP BY 1,
           2,
           3

)

SELECT
 a.blockchain,
 a.chain_id,
 a.block_id,
 a.block_timestamp,
 a.tx_id,
 msg_value :sender::STRING AS sender,
 COALESCE(action_log :borrower::STRING, action_contract_address) AS borrower,
 COALESCE(action_log :repay_amount / POW(10,6), 
 action_log :withdraw_amount_ust / POW(10,6)) AS amount,
 amount * p.price AS amount_usd,
 COALESCE(msg_value :coins [0] :denom :: STRING, 'uusd') AS currency,
 action_contract_address AS contract_address,
 l.address_name AS contract_label,
 CASE WHEN
 msg_value :execute_msg :process_anchor_message IS NOT NULL
 THEN 'Wormhole'
 ELSE 'Terra'
 END AS source
FROM {{ ref('silver_terra__event_actions') }} a

LEFT JOIN {{ ref('silver_terra__msgs') }} m
ON a.tx_id = m.tx_id AND a.msg_index = m.msg_index

LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON action_contract_address = l.address 
    AND l.blockchain = 'terra' 
    AND l.creator = 'flipside'
    
LEFT OUTER JOIN prices p
  ON DATE_TRUNC('hour', a.block_timestamp) = HOUR
  AND COALESCE(msg_value :coins [0] :denom :: STRING, 'uusd') = p.currency

WHERE action_method = 'repay_stable'
AND COALESCE(action_log :repay_amount, action_log :withdraw_amount_ust) IS NOT NULL
AND m.msg_index IS NOT NULL

{% if is_incremental() %}
AND a.block_timestamp :: DATE >= (SELECT MAX( block_timestamp :: DATE) FROM {{ ref('silver_terra__msgs') }})
{% endif %}
