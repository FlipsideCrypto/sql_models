{{
  config(
    materialized='incremental',
    sort='block_id',
    unique_key='tx_id || liquidated_amount || debt_to_cover_amount',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'aave', 'aave_liquidations']
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
        ethereum.reads
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


--liquidations to Aave LendingPool contract
liquidation AS(--need to fix aave v1
    SELECT
        DISTINCT block_id,
        block_timestamp,
        event_inputs:collateralAsset::string AS collateral_asset,
        event_inputs:debtAsset::string AS debt_asset,
        event_inputs:debtToCover AS debt_to_cover_amount, --not adjusted for decimals
        event_inputs:liquidatedCollateralAmount AS liquidated_amount,
        event_inputs:liquidator::string AS liquidator_address,
        event_inputs:user::string AS borrower_address,
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
        AND event_name = 'LiquidationCall' --this is a liquidation
        AND tx_succeeded = TRUE --excludes failed txs
        --AND TX_ID = '' --random tx
    --LIMIT 100
)

SELECT
    DISTINCT liquidation.block_id,
    liquidation.block_timestamp,
    liquidation.collateral_asset AS collateral_asset,
    prices.symbol AS collateral_asset_symbol,
    liquidation.debt_asset AS debt_asset,
    prices_2.symbol AS debt_asset_symbol,
    liquidation.debt_to_cover_amount / POW(10,prices_2.decimals) AS debt_to_cover_amount,
    liquidation.debt_to_cover_amount * prices_2.token_price / POW(10,prices_2.decimals) AS debt_to_cover_amount_usd,
    liquidation.liquidated_amount / POW(10,prices.decimals) AS liquidated_amount,
    liquidation.liquidated_amount * prices.token_price / POW(10,prices.decimals) AS liquidated_amount_usd,
    prices.token_address AS liquidated_address,
    prices.symbol AS liquidated_symbol,
    liquidation.liquidator_address AS liquidator,
    liquidation.borrower_address AS borrower,
    liquidation.tx_id,
    liquidation.aave_version
FROM
    liquidation
    LEFT JOIN prices
        ON liquidation.block_timestamp = prices.block_timestamp
        AND liquidation.collateral_asset = prices.token_address
    LEFT JOIN prices AS prices_2
        ON liquidation.block_timestamp = prices_2.block_timestamp
        AND liquidation.debt_asset = prices_2.token_address
WHERE 1=1
