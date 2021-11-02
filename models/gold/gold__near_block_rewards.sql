{{ config(
  materialized='table',
  cluster_by=['block_timestamp'],
  tags=['snowflake', 'gold', 'near', 'gold__near_rewards']
)}}
WITH validators AS (
  SELECT address
  FROM {{ ref('gold__near_validators') }}
  GROUP BY address  
), 
epoch_starts AS (
  SELECT 
    epoch_start_block,
    lag(epoch_start_block) over(order by epoch_start_block) as prior_epoch_start_block,
    lead(epoch_start_block) over(order by epoch_start_block) as next_epoch_start_block
  FROM {{ ref('gold__near_validators') }} 
  GROUP BY epoch_start_block ORDER BY epoch_start_block
),
-- get the prior epoch ending balances
epoch_prior_balances AS (
SELECT 
  address, 
  block_number + 1 AS epoch_start_block, 
  block_number AS prior_epoch_end, 
  stake_amount AS prior_epoch_stake,
  stake_amount_usd AS prior_epoch_stake_usd
FROM {{ ref('gold__near_validators') }} 
WHERE status = 'current_validators'
  AND block_number IN (SELECT epoch_start_block - 1 AS block_number FROM {{ ref('gold__near_validators') }})
),
-- now get each epoch start balance
epoch_balances AS (
SELECT 
  ep.address, 
  address_label_type,
  address_label_subtype,
  address_label,
  address_name,
  ep.epoch_start_block, 
  block_timestamp, 
  block_number, 
  stake_amount, 
  stake_amount_usd,
  pr.prior_epoch_end, 
  pr.prior_epoch_stake,
  pr.prior_epoch_stake_usd,
  ep.stake_amount - pr.prior_epoch_stake AS stake_balance_change,
  ep.stake_amount_usd - pr.prior_epoch_stake_usd AS stake_balance_change_usd
FROM {{ ref('gold__near_validators')}} ep
JOIN epoch_prior_balances pr ON ep.address = pr.address AND ep.epoch_start_block = pr.epoch_start_block
WHERE status = 'current_validators' AND ep.block_number = ep.epoch_start_block),
-- all stake and unstake events by block/validator
stake_unstakes AS (
  SELECT
  block_number,
  event_to as address,
  event_amount as amount,
  event_amount_usd as amount_usd
  FROM {{ ref('gold__near_events') }}
  WHERE 
  event_currency = 'NEAR'
  AND event_type = 'stake'
  AND event_to IN (SELECT address FROM validators)
UNION ALL
  SELECT
  block_number,
  event_from as address,
  event_amount * (-1) as amount,
  event_amount_usd * (-1) as amount_usd
  FROM {{ ref('gold__near_events') }}
  WHERE 
  event_currency = 'NEAR'
  AND event_type = 'unstake'
  AND event_from IN (SELECT address FROM validators)
),
-- stake event block number to matching epoch
-- do epoch numbers? 1 to whatever, then add 2?
net_stake_by_epoch AS (
  SELECT
  address,
  next_epoch_start_block AS epoch_start_block,
  sum(amount) as epoch_net_stake,
  sum(amount_usd) as epoch_net_stake_usd
  FROM stake_unstakes
  JOIN epoch_starts
  ON stake_unstakes.block_number > epoch_starts.prior_epoch_start_block
    AND stake_unstakes.block_number <= epoch_starts.epoch_start_block
  GROUP BY address, next_epoch_start_block
)
SELECT 
  'near' as blockchain,
  block_timestamp, 
  bals.epoch_start_block as block_number, 
  bals.address, 
  address_label_type,
  address_label_subtype,
  address_label,
  address_name,
  stake_balance_change - coalesce(epoch_net_stake, 0) AS block_reward,
  stake_balance_change_usd - coalesce(epoch_net_stake_usd, 0) AS block_reward_usd
FROM epoch_balances bals
LEFT OUTER JOIN net_stake_by_epoch nets
ON bals.address = nets.address
  AND bals.epoch_start_block = nets.epoch_start_block
