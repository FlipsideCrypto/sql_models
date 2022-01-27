{{ config(
  materialized = 'view',
  unique_key = 'join_date',
  tags = ['snowflake', 'terra', 'mirror', 'console']
) }}

WITH join_date_per_mirror_user AS (

  SELECT
    msg_value :sender :: STRING sender_address,
    MIN(DATE(block_timestamp)) join_date,
    DATEDIFF(DAY, MIN(DATE(block_timestamp)), CURRENT_DATE) user_age_days
  FROM
    {{ ref('terra__msgs') }}
  WHERE
    msg_value :contract = 'terra15gwkyepfc6xgca5t5zefzwy42uts8l2m4g40k6'
    AND DATE(block_timestamp) >= '2020-11-01'
  GROUP BY
    sender_address
  ORDER BY
    join_date
)
SELECT
  join_date,
  COUNT(sender_address) new_mirror_users
FROM
  join_date_per_mirror_user
WHERE
  join_date >= '2020-11-01'
GROUP BY
  join_date
ORDER BY
  join_date
