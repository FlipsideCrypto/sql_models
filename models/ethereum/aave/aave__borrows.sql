{{
  config(
    materialized='incremental',
    sort='block_id',
    unique_key='tx_id || loan_amount',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'aave', 'aave_borrows']
  )
}}


WITH
atokens AS(
    SELECT
        LOWER(inputs:_reserve::string) AS reserve_token,
        a.value::string AS balances
    FROM
        {{ref('ethereum__reads')}}
       ,lateral flatten(input => SPLIT(value_string,'^')) a
    WHERE 1=1
        AND block_timestamp::date >= '2021-05-01'
        AND contract_address  IN (
                LOWER('0xB53C1a33016B2DC2fF3653530bfF1848a515c8c5'),
                LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d'), -- AAVE V2
                LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
                LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba'),  -- AAVE AMM
                LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119')) -- AAVE V1
),

underlying AS(
    SELECT
        reserve_token AS token_contract,
        MAX(
          CASE
            WHEN SPLIT(balances,':')[0]::string = 'aTokenAddress' THEN SPLIT(balances,':')[1]
          ELSE '' END) AS aave_token
    FROM
        atokens
    WHERE 1=1
    GROUP BY 1
),

-- implementing aave oracle prices denominated in wei
oracle AS(
    SELECT
        block_timestamp,
        LOWER(inputs:address::string) AS token_address,
        value_numeric AS value_ethereum -- values are given in wei and need to be converted to ethereum
    FROM
        {{ref('ethereum__reads')}}
    WHERE 1=1
        AND contract_address = '0xa50ba011c48153de246e5192c8f9258a2ba79ca9' -- check if there is only one oracle
        AND block_timestamp::date >= '2021-05-01'
),

eth_prices AS(
    SELECT
        oracle.block_timestamp,
        oracle.token_address,
        oracle.value_ethereum,
        underlying.aave_token,
        symbols.symbol,
        symbols.decimals
    FROM
        oracle
        LEFT JOIN underlying
          ON oracle.token_address = underlying.token_contract
        LEFT JOIN {{ref('ethereum__token_prices_hourly')}} symbols
          ON oracle.token_address = symbols.token_address
          AND date_trunc('hour',oracle.block_timestamp) = symbols.hour
    WHERE 1=1
),



--pull hourly prices for each undelrying
prices AS (
    SELECT
        eth_prices.block_timestamp,
        (eth_prices.value_ethereum / POW(10,(CASE WHEN eth_prices.decimals IS NULL THEN 0 ELSE (18 -eth_prices.decimals) END))) * prices_hourly.price AS token_price,
        CASE WHEN eth_prices.decimals IS NULL THEN 18 ELSE eth_prices.decimals END AS decimals,
        eth_prices.symbol,
        eth_prices.token_address
    FROM
        eth_prices
        INNER JOIN ethereum.token_prices_hourly prices_hourly
          ON date_trunc('hour',eth_prices.block_timestamp) = prices_hourly.hour
          AND prices_hourly.hour::date >= '2021-05-01'
          AND prices_hourly.symbol = 'ETH'
    WHERE 1=1
),


--borrows from Aave LendingPool contract
borrow AS(
    SELECT
        DISTINCT block_id,
        block_timestamp,
        event_inputs:reserve AS aave_market,
        event_inputs:amount AS borrow_quantity, --not adjusted for decimals
        tx_from_address AS borrower_address,
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
        AND block_timestamp::date >= '2021-05-01'
        AND contract_address IN(--Aave V2 LendingPool contract address
            LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'),--V2
            LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119'),--V1
            LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'))--AMM
        AND event_name = 'Borrow' --this is a borrow
        AND tx_succeeded = TRUE --excludes failed txs
        --AND TX_ID = '' --random tx
    --LIMIT 100
)

SELECT
    DISTINCT borrow.block_id,
    borrow.block_timestamp,
    underlying.aave_token AS atoken,
    --atokens.project_name AS atoken_symbol,
    borrow.borrow_quantity / POW(10,prices.decimals) AS borrowed_atokens,
    borrow.borrow_quantity * prices.token_price / POW(10,prices.decimals) AS borrowed_usd,
    --prices.decimals AS aux_prices_decimals,
    prices.token_address AS borrowed_contract_address,
    prices.symbol AS borrowed_symbol,
    borrow.borrower_address AS borrower,
    borrow.tx_id,
    borrow.aave_version
FROM
    borrow
    LEFT JOIN prices
        ON borrow.block_timestamp = prices.block_timestamp
        AND borrow.aave_market = prices.token_address
    LEFT JOIN underlying
        ON prices.token_address = underlying.token_contract
WHERE 1=1
