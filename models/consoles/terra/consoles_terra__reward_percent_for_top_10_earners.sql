{{ config(
  materialized = 'view',
  unique_key = 'balance_date',
  tags = ['snowflake', 'console', 'terra', 'reward_percent']
) }}

SELECT
  ROUND(
    total_rewards_top10,
    2
  ) AS total_rewards_top10,
  ROUND(
    total_reward_amount,
    2
  ) AS total_reward_amount,
  ROUND(
    top10_percentage,
    2
  ) AS top10_percentage
FROM
  (
    SELECT
      SUM(amount) AS total_rewards_top10,
      (
        SELECT
          SUM(event_attributes :amount [0] :amount / pow(10, 6)) AS total_reward_amount
        FROM
          {{ ref('silver_terra__transitions') }}
        WHERE
          transition_type = 'end_block'
          AND event = 'rewards'
          AND block_timestamp >= CURRENT_DATE - 30
      ) AS total_reward_amount,
      total_rewards_top10 / total_reward_amount * 100 AS top10_percentage
    FROM
      (
        SELECT
          event_attributes :validator :: STRING AS validator,
          SUM(event_attributes :amount [0] :amount / pow(10, 6)) AS amount
        FROM
          {{ ref('silver_terra__transitions') }}
        WHERE
          transition_type = 'end_block'
          AND event = 'rewards'
          AND block_timestamp >= CURRENT_DATE - 30
        GROUP BY
          validator
        ORDER BY
          amount DESC
        LIMIT
          10
      ) A
  )
