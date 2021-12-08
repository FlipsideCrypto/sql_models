{{ config(
  materialized = 'view',
  unique_key = "CONCAT_WS('-', date, addresses)",
  tags = ['snowflake', 'console', 'terra', 'addresses_staking_LUNA']
) }}

WITH tmp AS(

  SELECT
    DATE,
    address,
    balance
  FROM
    {{ ref('terra__daily_balances') }}
  WHERE
    balance_type = 'staked'
    AND currency = 'LUNA'
    AND DATE :: DATE >= CURRENT_DATE - 60
)
SELECT
  DATE,
  COUNT(
    DISTINCT address
  ) AS addresses
FROM
  tmp
WHERE
  balance > 0
GROUP BY
  DATE
ORDER BY
  DATE DESC
