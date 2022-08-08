{{ config(
  materialized = 'view',
  post_hook = "call silver_algorand.sp_bulk_get_tx()"
) }}

WITH tx AS (

  SELECT
    sender AS account_id,
    tx_id
  FROM
    {{ ref("core__fact_transaction") }}
  WHERE
    block_timestamp :: DATE > '2022-07-27' qualify (ROW_NUMBER() over (PARTITION BY tx_id
  ORDER BY
    sender)) = 1
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
  {{ ref("silver__indexer_tx") }}
