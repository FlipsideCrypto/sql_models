{{
  config(
    materialized='incremental',
    sort='block_id',
    unique_key='tx_id || issued_atokens',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'aave', 'aave_deposits']
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
    FROM {{ref('ethereum__events_emitted')}}
    WHERE 1=1
        AND contract_address IN (
            LOWER('0xB9D7CB55f463405CDfBe4E90a6D2Df01C2B92BF1'),
            LOWER('0xBcca60bB61934080951369a648Fb03DF4F96263C'))
        AND block_timestamp::date = '2021-06-12' --= getdate() - INTERVAL '131 days'
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
        {{ref('ethereum__token_prices_hourly')}} prices
        INNER JOIN underlying ON prices.token_address = underlying.token_contract
    WHERE 1=1
        AND (symbol = 'ETH'
             OR token_address IN(--need to edit to get the list of underlying tokens
                SELECT DISTINCT token_contract FROM underlying))
        AND hour::date = '2021-06-12' --= getdate() - INTERVAL '131 days'
    GROUP BY
        prices.hour,
        prices.symbol,
        prices.token_address,
        underlying.aave_token
),

--deposits into Aave LendingPool contract
deposits AS(
    SELECT
        DISTINCT block_id,
        block_timestamp,
        event_inputs:reserve::string AS aave_market,
        event_inputs:amount AS deposit_quantity, --not adjusted for decimals
        tx_from_address AS depositor_address,
        tx_to_address AS lending_pool_contract,
        tx_id,
        CASE
            WHEN contract_address = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN contract_address = LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') THEN 'Aave V1'
            WHEN contract_address = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
          ELSE 'ERROR' END AS aave_version
    FROM
        {{ref('ethereum__events_emitted')}}
    WHERE 1=1
        AND block_timestamp::date = '2021-06-12' --= getdate() - INTERVAL '131 days'
        AND contract_address IN(--Aave V2 LendingPool contract address
            LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'),--V2
            LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119'),--V1
            LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'))--AMM
        AND event_name = 'Deposit' --this is a deposit
        AND tx_succeeded = TRUE --excludes failed txs
        --AND TX_ID = '0x86630414e34ad67f82b1715b3b59c4613fddf2a8915a47e9a77711707951a8f0' --random Aave deposit tx
    --LIMIT 100
)--,

--aux AS (
SELECT
    DISTINCT deposits.block_id,
    deposits.block_timestamp,
    deposits.aave_market AS atoken,
    atokens.project_name AS atoken_symbol,
    deposits.deposit_quantity / POW(10,prices.token_decimals) AS issued_atokens,
    deposits.deposit_quantity * prices.token_price / POW(10,prices.token_decimals) AS supplied_usd,
    prices.token_contract AS supplied_contract_address,
    prices.symbol AS supplied_symbol,
    deposits.depositor_address AS supplier,
    deposits.tx_id,
    deposits.aave_version
FROM
    deposits
    LEFT JOIN prices
        ON date_trunc('hour',deposits.block_timestamp) = prices.block_hour
        AND deposits.aave_market = prices.token_contract
    LEFT JOIN atokens
        ON prices.aave_token = atokens.address
WHERE
    block_timestamp::date = '2021-06-12' --= getdate() - INTERVAL '131 days'
/*)

SELECT --* FROM aux WHERE tx_id = '0x336b64109e66bf7135d30e820e6e35ca16ecd0f089ca1e80fbd7473a5b9bccc2'
    tx_id,
    issued_atokens,
    COUNT(1)
FROM
    aux
GROUP BY 1,2
HAVING COUNT(1) > 1
ORDER BY 3 DESC*/
