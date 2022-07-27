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
    qualify (ROW_NUMBER() over (PARTITION BY tx_id
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
  {{ ref("silver_algorand__indexer_tx") }}
