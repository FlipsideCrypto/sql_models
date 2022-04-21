{{ config(
  materialized = 'view'
) }}

SELECT
  sender AS account,
  tx_id AS txid
FROM
  {{ ref("silver_algorand__transactions") }}
  -- EXCEPT
  -- SELECT
  --   address,
  --   tx_id
  -- FROM
  --   {{ ref("silver_algorand__transactions") }}
ORDER BY
  block_timestamp DESC
LIMIT
  1000000
