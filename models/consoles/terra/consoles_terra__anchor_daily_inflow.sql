{{ config(
  materialized = 'view',
  unique_key = 'join_date',
  tags = ['snowflake', 'terra', 'anchor', 'console', 'anchor_daily']
) }}

WITH join_date_per_anchor_user AS (

  SELECT
    msg_value :sender :: STRING sender_address,
    MIN(DATE(block_timestamp)) join_date,
    DATEDIFF(DAY, MIN(DATE(block_timestamp)), CURRENT_DATE) user_age_days
  FROM
    {{ ref('terra__msgs') }}
  WHERE
    msg_value :contract = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
    AND DATE(block_timestamp) >= '2020-11-01'
  GROUP BY
    sender_address
  ORDER BY
    join_date
)
SELECT
  join_date,
  COUNT(sender_address) new_anchor_users
FROM
  join_date_per_anchor_user
WHERE
  join_date >= '2021-01-01'
GROUP BY
  join_date
ORDER BY
  join_date
