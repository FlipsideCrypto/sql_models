{{
  config(
    materialized='incremental',
    sort='block_id',
    unique_key='tx_id || repayed_amount',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'aave', 'aave_repayments']
  )
}}


WITH
atokens as (
    SELECT
        DISTINCT contract_address AS address,
        CASE --to be substituted for a read
            WHEN contract_address = LOWER('0xB9D7CB55f463405CDfBe4E90a6D2Df01C2B92BF1') THEN 'aUNI'
            WHEN contract_address = LOWER('0xBcca60bB61934080951369a648Fb03DF4F96263C') THEN 'aUSDC'
            END AS project_name
    FROM ethereum.events_emitted
    WHERE 1=1
        AND contract_address IN (
            LOWER('0xB9D7CB55f463405CDfBe4E90a6D2Df01C2B92BF1'),
            LOWER('0xBcca60bB61934080951369a648Fb03DF4F96263C'))
        AND block_timestamp > GETDATE() - INTERVAL '31 days'
),

--fake underlying for ilustrational porposes
underlying AS(
    SELECT
        DISTINCT address AS aave_token,
        CASE
            WHEN project_name = 'aUNI' THEN '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'
            WHEN project_name = 'aUSDC' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
            ELSE 'Other'
          END AS token_contract
    FROM atokens
),

--pull hourly prices for each undelrying
prices AS (
    SELECT
        prices.hour as block_hour,
        MAX(prices.price) as token_price,
        MAX(prices.decimals) as token_decimals,
        prices.symbol,
        prices.token_address as token_contract, -- this is the underlying asset
        underlying.aave_token-- this is the atoken
    FROM
        ethereum.token_prices_hourly prices
        INNER JOIN underlying ON prices.token_address = underlying.token_contract
    WHERE 1=1
        AND (symbol = 'ETH'
             OR token_address IN(--need to edit to get the list of underlying tokens
                SELECT DISTINCT token_contract FROM underlying))
        AND hour > GETDATE() - INTERVAL '31 days'
    GROUP BY
        prices.hour,
        prices.symbol,
        prices.token_address,
        underlying.aave_token
),

--repayments to Aave LendingPool contract
repay AS(
    SELECT
        DISTINCT block_id,
        block_timestamp,
        event_inputs:vault::string AS aave_market,
        event_inputs:amount AS repayed_amount, --not adjusted for decimals
        tx_from_address AS repayer_address,
        event_inputs:owner::string AS borrower_address,
        tx_to_address AS lending_pool_contract,
        tx_id,
        CASE
            WHEN contract_address = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN contract_address = LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') THEN 'Aave V1'
            WHEN contract_address = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
          ELSE 'ERROR' END AS aave_version
    FROM
        ethereum.events_emitted
    WHERE 1=1
        AND block_timestamp > GETDATE() - INTERVAL '31 days'
        AND contract_address IN(--Aave V2 LendingPool contract address
            LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'),--V2
            LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119'),--V1
            LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'))--AMM
        AND event_name = 'Repay' --this is a repayment
        AND tx_succeeded = TRUE --excludes failed txs
        --AND TX_ID = '' --random tx
    --LIMIT 100
)

SELECT
    DISTINCT repay.block_id,
    repay.block_timestamp,
    repay.aave_market AS atoken_address,
    atokens.project_name AS atoken_symbol,
    repay.repayed_amount / POW(10,prices.token_decimals) AS repayed_amount,
    repay.repayed_amount * prices.token_price / POW(10,prices.token_decimals) AS loan_amount_usd,
    prices.token_contract AS repayed_address,
    prices.symbol AS repayed_symbol,
    repay.repayer_address AS payer,
    repay.borrower_address AS borrower,
    repay.tx_id,
    repay.aave_version
FROM
    repay
    LEFT JOIN prices
        ON date_trunc('hour',repay.block_timestamp) = prices.block_hour
        AND repay.aave_market = prices.token_contract
    LEFT JOIN atokens
        ON prices.aave_token = atokens.address
WHERE 1=1
    AND repay.block_timestamp >= getdate() - interval '31 days'
