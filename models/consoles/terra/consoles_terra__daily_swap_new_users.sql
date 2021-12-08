{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terra_daily_swap_new_users']
) }}

SELECT
  DATE_TRUNC(
    'day',
    block_timestamp
  ) AS block_day,
  COUNT(DISTINCT(msg_value :sender :: STRING)) AS sender
FROM
  {{ ref('terra__msgs') }}
WHERE
  msg_value :contract = 'terra1tndcaqxkpc5ce9qee5ggqf430mr2z3pefe5wj6'
  AND DATE_TRUNC(
    'day',
    block_timestamp
  ) >= '2021-01-01'
GROUP BY
  block_day
ORDER BY
  block_day DESC
