{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', join_date, new_anchor_users)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['join_date'],
  tags = ['snowflake', 'terra', 'anchor', 'console', 'anchor_daily']
) }}


WITH join_date_per_anchor_user as (
SELECT
  msg_value:sender::string sender_address,
  MIN(DATE(block_timestamp)) join_date,
  DATEDIFF(day, MIN(DATE(block_timestamp)), CURRENT_DATE) user_age_days
FROM {{ ref('terra__msgs') }}
WHERE msg_value:contract = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s' -- contract ID for anchor
    and DATE(block_timestamp) >= '2020-11-01'
GROUP BY 1
ORDER BY 2
)

SELECT
  join_date,
  COUNT(sender_address) new_anchor_users -- Addresses are already unique
FROM
  join_date_per_anchor_user
WHERE join_date >= '2021-01-01' -- We'll focus on 2021
GROUP BY 1
ORDER BY 1