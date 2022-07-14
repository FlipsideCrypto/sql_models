{{ config(
  materialized = 'view',
  post_hook = "call silver_algorand.sp_bulk_get_tx()"
) }}

WITH tx AS (

  SELECT
    sender AS account_id,
    tx_id,
    block_id
  FROM
    {{ ref("silver_algorand__transactions") }}
),
z AS (
  SELECT
    account_id,
    tx_id,
    block_id
  FROM
    tx
  EXCEPT
  SELECT
    account_id,
    tx_id,
    block_id
  FROM
    {{ ref("silver_algorand__indexer_tx") }}
  ORDER BY
    block_id
)
SELECT
  account_id,
  tx_id
FROM
  z
