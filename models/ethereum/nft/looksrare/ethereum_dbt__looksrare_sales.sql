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
        COALESCE(
            event_inputs :from :: STRING,
            event_inputs :_from :: STRING
        ) AS seller,
        COALESCE(
            event_inputs :to :: STRING,
            event_inputs :_to :: STRING
        ) AS buyer,
        COALESCE(
            event_inputs :tokenId,
            event_inputs :_id,
            event_inputs :id
        ) AS token_id
    FROM
        {{ ref('silver_ethereum__events_emitted') }}
    WHERE
        event_name IN (
            'Transfer',
            'TransferSingle'
        )
        AND tx_id IN (
            SELECT
                tx_hash
            FROM
                {{ ref('silver_ethereum__events') }}
            WHERE
                contract_address = '0x59728544b08ab483533076417fbbb2fd0b17ce3a'
        )
        AND token_id IS NOT NULL
{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
),
-- -- filter out intermediate addresses
-- token_transfers as (
--     select 
--         tx_id,
--         contract_address,
--         block_timestamp,
--         token_id,
--         max(case when seller = '0x677a2749082fcfbe0ae12300e2c0c1309a50e81f' then null else seller end) as seller,
--         max(case when buyer = '0x677a2749082fcfbe0ae12300e2c0c1309a50e81f' then null else buyer end) as buyer
--     from token_transfers_tmp
--     group by 1,2,3,4
-- ),
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
        event_id,
        origin_address,
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
                {{ ref('ethereum__events_emitted') }}
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
        (from_address = buyer or origin_address = buyer)
    AND 
        to_address = seller
    qualify(row_number() over (partition by tx_id, origin_address, from_address, to_address order by event_id desc)) = 1
),
-- creator_fee AS (
--     SELECT
--         tx_id,
--         amount AS creator_fee
--     FROM
--         token_transfer_events
--     WHERE
--         (from_address = buyer or origin_address = buyer)
--     AND 
--         to_address <> '0x5924a28caaf1cc016617874a2f0c3710d881f3c1'
--     qualify(row_number() over (partition by tx_id, origin_address, from_address, to_address order by event_id)) = 1
-- ),
-- find how much is paid to looksrare
platform_fees AS (
    SELECT
        tx_id,
        amount AS platform_fee
    FROM
        token_transfer_events
    WHERE
        to_address = '0x5924a28caaf1cc016617874a2f0c3710d881f3c1'
) -- we're joining on the original NFT transfers CTE
SELECT
    'looksrare' AS event_platform,
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
    -- COALESCE(
    --     cf.creator_fee / n_tokens,
    --     0
    -- ) AS creator_fee,
    0 as creator_fee,
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
    -- LEFT OUTER JOIN creator_fee cf
    -- ON tt.tx_id = cf.tx_id
WHERE
    tt.tx_id <> '0xc5fd70e0f59961e5e73453060f547c098535365035f66b003e301339eb288c70'
