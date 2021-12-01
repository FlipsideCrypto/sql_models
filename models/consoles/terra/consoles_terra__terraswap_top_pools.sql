{{ config(
  materialized = 'view',
  tags = ['snowflake', 'terra', 'console', 'terraswap_top_pools']
) }}

SELECT
  REGEXP_SUBSTR(
    address_name,
    ' (.*) '
  ) AS contract_label,
  msg_value :contract :: STRING AS contract_address,
  COUNT(
    DISTINCT tx_id
  ) AS tradeCount,
  COUNT(
    DISTINCT msg_value :sender :: STRING
  ) AS addressCount,
  SUM(
    (
      NVL((msg_value :execute_msg :swap :offer_asset :amount), 0) / pow(
        10,
        6
      )
    )
  ) AS volumetokenamount
FROM
  {{ ref('terra__msgs') }}
  LEFT OUTER JOIN {{ ref('terra__labels') }} C
  ON msg_value :contract :: STRING = C.address
WHERE
  msg_value :execute_msg :swap IS NOT NULL
  AND contract_label IS NOT NULL
  AND block_timestamp >= CURRENT_DATE - 7
GROUP BY
  contract_label,
  contract_address
ORDER BY
  tradeCount DESC
LIMIT
  5
