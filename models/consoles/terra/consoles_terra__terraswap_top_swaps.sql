{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terraswap_top_swaps']
) }}
--queryId: c3b3482b-2aa0-4797-8143-ec80c4eac043

with native as (
select 
  		replace(swap_pair,' to ','-') as swap_pair,
    	COUNT(DISTINCT tx_id) AS tradeCount,
    	COUNT(DISTINCT trader) as addressCount,
    	sum(nvl(token_0_amount,0)) as VolumeTokenAmount,
    	sum(nvl(token_0_amount_usd,0)) as VolumeToken0usd
  
from 	 {{ ref('terra__swaps') }}
where	 swap_pair is not null
group by swap_pair
order by VolumeToken0usd desc
limit 5
)

select * from native