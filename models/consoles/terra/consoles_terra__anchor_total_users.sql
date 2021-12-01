{{ config(
  materialized = 'view',
  unique_key = 'block_date',
  tags = ['snowflake', 'terra', 'anchor', 'console']
) }}

WITH anchor_contracts AS (

  SELECT
    address,
    address_name,
    blockchain,
    label,
    label_subtype,
    label_type
  FROM
    {{ ref('terra__labels') }}
  WHERE
    label :: STRING = 'anchor'
  ORDER BY
    label_type DESC
),
daily_interactions AS (
  SELECT
    DATE(block_timestamp) AS block_date,
    COUNT(
      DISTINCT msg_value :sender :: STRING
    ) AS sender_count
  FROM
    {{ ref('terra__msgs') }}
    m
    INNER JOIN anchor_contracts ac
    ON ac.address = m.msg_value :contract
  GROUP BY
    block_date
)
SELECT
  block_date,
  SUM(sender_count) over (
    ORDER BY
      block_date ASC
  ) total_users
FROM
  daily_interactions
ORDER BY
  block_date DESC
