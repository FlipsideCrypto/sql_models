{{ config(
  materialized = 'view',
  post_hook = "call silver_algorand.sp_bulk_get_tx()"
) }}

WITH tx AS (

  SELECT
    sender AS account_id,
    tx_id
  FROM
    {{ ref("silver_algorand__transactions") }}
  WHERE
    block_timestamp >= '2022-04-01'
  ORDER BY
    block_timestamp DESC
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
    "algorand_db_external",
    "algorand_indexer_tx"
  ) }}
