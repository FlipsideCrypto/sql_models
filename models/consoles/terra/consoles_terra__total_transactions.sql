{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terra_total_transactions']
) }}

SELECT
  DATE(block_timestamp) AS DAY,
  COUNT(
    DISTINCT tx_id
  ) AS tx_count
FROM
  {{ ref('terra__msgs') }}
WHERE
  DAY <= CURRENT_DATE
GROUP BY
  DAY
ORDER BY
  DAY DESC
