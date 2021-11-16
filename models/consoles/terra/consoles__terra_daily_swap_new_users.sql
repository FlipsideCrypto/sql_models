{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terra_daily_swap_new_users']
) }}
--queryId: a33a0d60-ab6f-4cc1-8232-40694204c2c1
SELECT 
date_trunc('day',block_timestamp) as block_day,
count(DISTINCT(msg_value:sender::string)) as sender

FROM
{{ ref('terra__msgs') }}

WHERE msg_value:contract = 'terra1tndcaqxkpc5ce9qee5ggqf430mr2z3pefe5wj6'
and date_trunc('day',block_timestamp) >= '2021-01-01'

group by block_day
order by block_day desc
