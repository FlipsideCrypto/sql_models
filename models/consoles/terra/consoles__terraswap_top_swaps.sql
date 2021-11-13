{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terraswap_top_swaps']
) }}
--queryId: c3b3482b-2aa0-4797-8143-ec80c4eac043

with native as (
select  --date_trunc('day', block_timestamp) AS Date,
  		replace(swap_pair,' to ','-') as swap_pair,
    	COUNT(DISTINCT tx_id) AS tradeCount,
    	COUNT(DISTINCT trader) as addressCount,
    	sum(nvl(token_0_amount,0)) as VolumeTokenAmount,
    	sum(nvl(token_0_amount_usd,0)) as VolumeToken0usd
  
from 	 {{ ref('terra__swaps') }}
where	 swap_pair is not null
group by 1
  order by 5 desc
  limit 5
),

  terraswap as (
  SELECT
  --date_trunc('day', block_timestamp) AS Date,
  COUNT(DISTINCT tx_id) AS tradeCount,
  COUNT(DISTINCT msg_value:sender::string) as addressCount,
  sum((nvl((msg_value:execute_msg:swap:offer_asset:amount),0) / POW(10,6))) as VolumeTokenAmount,
  msg_value:execute_msg:swap:offer_asset:info:native_token:denom::string as swap_currency,
  msg_value:contract::string as contract_address,
  REGEXP_SUBSTR(address_name, ' (.*) ') as contract_label
FROM {{ ref('terra__msgs') }}

LEFT OUTER JOIN {{ ref('terra__labels') }} c
  ON msg_value:contract::string = c.address

WHERE msg_value:execute_msg:swap IS NOT NULL
  AND contract_label is not null
  AND block_timestamp >= CURRENT_DATE - 7
  group by 4,5,6
  )
  
/*select n.swap_pair,
  		t.contract_label,
  		n.tradecount,
  		n.addresscount,
  		n.VolumeTokenAmount,
  		t.tradecount,
  		t.addresscount,
  		t.VolumeTokenAmount
  from native n LEFT OUTER JOIN terraswap t
	ON n.swap_pair = t.contract_label*/
select * from native