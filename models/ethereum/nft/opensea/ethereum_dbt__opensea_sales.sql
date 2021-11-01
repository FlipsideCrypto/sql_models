{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}
-- first get every NFT transfer that happens
-- in a transaction with the AtomicMatch function:
WITH token_transfers AS (

  SELECT
    tx_id,
    contract_addr AS contract_address,
    block_timestamp,
    event_inputs :from AS seller,
    event_inputs :to AS buyer,
    event_inputs :tokenId AS token_id
  FROM
    {{ ref('silver_ethereum__events_emitted') }}
  WHERE
    event_name = 'Transfer'
    AND tx_id IN (
      SELECT
        tx_hash
      FROM
        silver.ethereum_events
      WHERE
        input_method = '0xab834bab'
    )
    AND token_id IS NOT NULL

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  tx_id,
  contract_addr AS contract_address,
  block_timestamp,
  event_inputs :_from AS seller,
  event_inputs :_to AS buyer,
  event_inputs :_id AS token_id
FROM
  {{ ref('silver_ethereum__events_emitted') }}
WHERE
  event_name = 'TransferSingle'
  AND tx_id IN (
    SELECT
      tx_hash
    FROM
      silver.ethereum_events
    WHERE
      input_method = '0xab834bab'
  )
  AND token_id IS NOT NULL

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  tx_id,
  contract_addr AS contract_address,
  block_timestamp,
  event_inputs :_from AS seller,
  event_inputs :_to AS buyer,
  event_inputs :_tokenId AS token_id
FROM
  {{ ref('silver_ethereum__events_emitted') }}
WHERE
  event_name = 'Transfer'
  AND tx_id IN (
    SELECT
      tx_hash
    FROM
      silver.ethereum_events
    WHERE
      input_method = '0xab834bab'
  )
  AND token_id IS NOT NULL

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  tx_id,
  contract_addr AS contract_address,
  block_timestamp,
  event_inputs :from AS seller,
  event_inputs :to AS buyer,
  event_inputs :id AS token_id
FROM
  {{ ref('silver_ethereum__events_emitted') }}
WHERE
  event_name = 'TransferSingle'
  AND tx_id IN (
    SELECT
      tx_hash
    FROM
      silver.ethereum_events
    WHERE
      input_method = '0xab834bab'
  )
  AND token_id IS NOT NULL

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
),
-- count how many tokens are in the txn
nfts_per_tx AS (
  SELECT
    tx_id,
    COUNT(token_id) AS n_tokens
  FROM
    token_transfers
  GROUP BY
    tx_id
),
-- get the buyer and the seller from
-- who is on the from/to sides of the
-- token transfers
tx_buyer_seller AS (
  SELECT
    tx_id,
    buyer,
    seller
  FROM
    token_transfers
  WHERE
    token_id IS NOT NULL
  GROUP BY
    tx_id,
    buyer,
    seller
),
-- now find the fungible token transfers
token_transfer_events AS (
  SELECT
    ee.tx_id,
    from_address,
    contract_address,
    buyer,
    seller,
    to_address,
    symbol,
    amount
  FROM
    {{ ref('ethereum__udm_events') }}
    ee
    JOIN tx_buyer_seller tbs
    ON ee.tx_id = tbs.tx_id
  WHERE
    ee.tx_id IN (
      SELECT
        tx_id
      FROM
        token_transfers
      WHERE
        token_id IS NOT NULL
    )
    AND amount > 0
    AND block_timestamp > (
      SELECT
        MIN(block_timestamp)
      FROM
        ethereum.events_emitted
    )

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
),
-- find the amount paid
tx_paid AS (
  SELECT
    tx_id,
    amount AS price,
    symbol AS tx_currency,
    contract_address AS tx_currency_contract
  FROM
    token_transfer_events
  WHERE
    from_address = buyer
),
-- find how much is paid to opensea
platform_fees AS (
  SELECT
    tx_id,
    amount AS platform_fee
  FROM
    token_transfer_events
  WHERE
    to_address = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
) -- we're joining on the original NFT transfers CTE
SELECT
  'opensea' AS event_platform,
  tt.tx_id,
  tt.block_timestamp,
  'sale' AS event_type,
  tt.contract_address,
  token_id,
  seller AS event_from,
  buyer AS event_to,
  price / n_tokens AS price,
  COALESCE(
    platform_fee / n_tokens,
    0
  ) AS platform_fee,
  0 AS creator_fee,
  CASE
    WHEN tx_currency IS NULL THEN tx_currency_contract
    ELSE tx_currency
  END AS tx_currency
FROM
  token_transfers tt
  LEFT OUTER JOIN tx_paid
  ON tt.tx_id = tx_paid.tx_id
  LEFT OUTER JOIN platform_fees
  ON tt.tx_id = platform_fees.tx_id
  LEFT OUTER JOIN nfts_per_tx
  ON tt.tx_id = nfts_per_tx.tx_id
