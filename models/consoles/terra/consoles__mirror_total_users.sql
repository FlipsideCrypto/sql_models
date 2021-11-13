{{ config(
  materialized = 'view',
  unique_key = 'block_date',
  tags = ['snowflake', 'terra', 'mirror', 'console']
) }}

WITH MIRROR_CONTRACTS AS (SELECT 
  address,
  address_name,
  blockchain,
  label,
  label_subtype,
  label_type
FROM {{ ref('terra__labels') }}
WHERE label::string = 'mirror'
ORDER BY label_type DESC), 
DAILY_INTERACTIONS AS (SELECT 
  date(block_timestamp) as block_date,
  count(distinct msg_value:sender::string) as sender_count
FROM {{ ref('terra__msgs') }} m
INNER JOIN MIRROR_CONTRACTS ac
ON ac.address = m.msg_value:contract
GROUP BY block_date)
select
  block_date,
  sum(sender_count) over (order by block_date asc)  total_users 
from daily_interactions
ORDER BY block_date desc