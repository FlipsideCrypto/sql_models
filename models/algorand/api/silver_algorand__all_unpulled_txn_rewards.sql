{{ config(
  materialized = 'view'
) }}

WITH tx AS (

  SELECT
    sender AS account_id,
    tx_id
  FROM
    {{ ref("silver_algorand__transactions") }}
)
SELECT
  account_id,
  tx_id
FROM
  tx
EXCEPT
SELECT
  account_id,
  tx_id
FROM
  {{ source(
    "external_tables",
    "algorand_txn_rewards"
  ) }}
