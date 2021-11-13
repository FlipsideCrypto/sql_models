{{ config(
  materialized = 'view',
  unique_key = 'block_date',
  tags = ['snowflake', 'terra', 'anchor', 'console']
) }}

WITH ANCHOR_CONTRACTS AS (SELECT 
  *
FROM {{ ref('terra__labels') }}
WHERE label::string = 'anchor'
ORDER BY label_type DESC), 
DAILY_INTERACTIONS AS (SELECT 
  date(block_timestamp) as block_date,
  count(distinct msg_value:sender::string) as sender_count
FROM {{ ref('terra__msgs') }} m
INNER JOIN ANCHOR_CONTRACTS ac
ON ac.address = m.msg_value:contract
GROUP BY block_date)
select
  block_date,
  sum(sender_count) over (order by block_date asc) total_users 
from daily_interactions
ORDER BY block_date desc
