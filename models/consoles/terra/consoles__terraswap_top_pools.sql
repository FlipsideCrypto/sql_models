{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terraswap_top_pools']
) }}
--queryId: 25f05359-e828-49e1-887c-ac75fe384c53

SELECT
  
  REGEXP_SUBSTR(address_name, ' (.*) ') as contract_label,
  msg_value:contract::string as contract_address,
  COUNT(DISTINCT tx_id) AS tradeCount,
  COUNT(DISTINCT msg_value:sender::string) as addressCount,
  sum((nvl((msg_value:execute_msg:swap:offer_asset:amount),0) / POW(10,6))) as VolumeTokenAmount
  
FROM {{ ref('terra__msgs') }}

LEFT OUTER JOIN {{ ref('terra__labels') }} c
  ON msg_value:contract::string = c.address
WHERE msg_value:execute_msg:swap IS NOT NULL
  AND contract_label is not null
  AND block_timestamp >= CURRENT_DATE - 7
  group by contract_label, contract_address
order by tradeCount DESC
LIMIT 5