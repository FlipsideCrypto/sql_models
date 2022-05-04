{{ config(
  materialized = 'view'
) }}

SELECT
  sender AS account,
  tx_id AS txid
FROM
  {{ ref("silver_algorand__transactions") }}
  {# EXCEPT
SELECT
  address,
  tx_id
FROM
  {{ source(
    "bronze",
    "algorand_api"
  ) }}
  #}
ORDER BY
  block_timestamp DESC
LIMIT
  10000
