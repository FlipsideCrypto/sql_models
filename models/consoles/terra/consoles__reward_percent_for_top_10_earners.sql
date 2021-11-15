{{ 
  config(
    materialized='view', 
    unique_key='balance_date',  
    tags=['snowflake', 'console', 'terra', 'reward_percent']
  )
}}

select
  round(total_rewards_top10,2) as total_rewards_top10,
  round(total_reward_amount,2) as total_reward_amount,
  round(top10_percentage,2) as top10_percentage
  from
(
select sum(amount) as total_rewards_top10
, (SELECT 
sum(event_attributes:amount[0]:amount/ POW(10,6)) as total_reward_amount
FROM {{ ref('silver_terra__transitions') }}

WHERE 
  	TRANSITION_TYPE = 'end_block' 
  	AND EVENT = 'rewards'
  	AND BLOCK_TIMESTAMP >= CURRENT_DATE - 30) as total_reward_amount
  , total_rewards_top10/total_reward_amount * 100 as top10_percentage
  
  FROM
(
SELECT 
	event_attributes:validator::string  as validator,
	sum(event_attributes:amount[0]:amount/ POW(10,6)) as amount
  
FROM {{ ref('silver_terra__transitions') }}

WHERE 
  	TRANSITION_TYPE = 'end_block' 
  	AND EVENT = 'rewards'
  	AND BLOCK_TIMESTAMP >= CURRENT_DATE - 30

GROUP BY validator
ORDER BY amount DESC

LIMIT 10 

 ) a
)