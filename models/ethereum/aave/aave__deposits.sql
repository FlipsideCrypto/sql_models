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
atokens AS(
    SELECT
        inputs:_reserve::string AS reserve_token,
        a.value::string AS balances,
        CASE
            WHEN contract_address IN(
                LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'),
                LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d')) THEN 'Aave V2'
            WHEN contract_address IN(
                LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
                LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba')) THEN 'Aave AMM'
            ELSE 'Aave V1'
          END AS aave_version
    FROM
        {{ref('ethereum__reads')}}
       ,lateral flatten(input => SPLIT(value_string,'^')) a
    WHERE 1=1
        AND block_timestamp::date >= '2021-06-01'
        AND contract_address  IN (
            LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d'), -- AAVE V2 Data Provider (per docs)
            LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'), -- AAVE AMM Lending Pool (per docs)
            LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'), -- AAVE V2 Lending Pool (per docs)
            LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba'),  -- AAVE AMM Data Provider (per docs)
            LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119')) -- AAVE V1
),


underlying AS(
    SELECT
        CASE
            WHEN reserve_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
              ELSE reserve_token END AS token_contract,
        aave_version,
        MAX(
          CASE
            WHEN SPLIT(balances,':')[0]::string = 'aTokenAddress' THEN SPLIT(balances,':')[1]
          ELSE '' END) AS aave_token
    FROM
        atokens
    WHERE 1=1
    GROUP BY 1,2
),


-- implementing aave oracle prices denominated in wei
oracle AS(
    SELECT
        --block_timestamp,
        date_trunc('hour',block_timestamp) AS block_hour,
        inputs:address::string AS token_address,
        AVG(value_numeric) AS value_ethereum -- values are given in wei and need to be converted to ethereum
    FROM
        {{ref('ethereum__reads')}}
    WHERE 1=1
        AND contract_address = '0xa50ba011c48153de246e5192c8f9258a2ba79ca9' -- check if there is only one oracle
        AND block_timestamp::date >= '2021-01-01'
    GROUP BY 1,2
),

-- wen we don't have oracle pricces we use ethereum__token_prices_hourly as a backup
backup_prices AS(
    SELECT
        token_address,
        hour,
        decimals,
        CASE WHEN symbol = 'KNCL' THEN 'KNC' ELSE symbol END AS symbol,
        AVG(price) AS price -- table has duplicated rows for KNC / KNCL so we need to do a trick
    FROM
        {{ref('ethereum__token_prices_hourly')}}
    WHERE 1=1
        AND hour::date >= '2021-01-01'
    GROUP BY 1,2,3,4
),


prices_hourly AS(
    SELECT
        underlying.aave_token,
        underlying.token_contract,
        underlying.aave_version,
        (oracle.value_ethereum / POW(10,(18 - backup_prices.decimals))) * eth_prices.price AS oracle_price,
        backup_prices.price AS backup_price,
        oracle.block_hour AS oracle_hour,
        backup_prices.hour AS backup_prices_hour,
        eth_prices.price AS eth_price,
        backup_prices.decimals AS decimals,
        backup_prices.symbol
    FROM
        underlying
        LEFT JOIN oracle
            ON LOWER(underlying.token_contract) = LOWER(oracle.token_address)
        LEFT JOIN backup_prices
            ON LOWER(underlying.token_contract) = LOWER(backup_prices.token_address)
            AND oracle.block_hour = backup_prices.hour
        LEFT JOIN {{ref('ethereum__token_prices_hourly')}} eth_prices
            ON oracle.block_hour = eth_prices.hour
            AND eth_prices.hour::date >= '2021-01-01'
            AND eth_prices.symbol = 'ETH'
),


coalesced_prices AS(
    SELECT
        prices_hourly.decimals AS decimals,
        prices_hourly.symbol AS symbol,
        prices_hourly.aave_token AS aave_token,
        prices_hourly.token_contract AS token_contract,
        prices_hourly.aave_version AS aave_version,
        COALESCE(prices_hourly.oracle_price,prices_hourly.backup_price) AS coalesced_price,
        COALESCE(prices_hourly.oracle_hour,prices_hourly.backup_prices_hour) AS coalesced_hour
    FROM
        prices_hourly
),

-- daily avg price used when hourly price is missing (it happens a lot)
prices_daily_backup AS(
    SELECT
        token_address,
        CASE WHEN symbol = 'KNCL' THEN 'KNC' ELSE symbol END AS symbol,
        date_trunc('day',hour) AS block_date,
        AVG(price) AS avg_daily_price,
        MAX(decimals) AS decimals
    FROM
        backup_prices
    WHERE 1=1
    GROUP BY 1,2,3
),

-- decimals backup
decimals_backup AS(
    SELECT
        address AS token_address,
        meta:decimals AS decimals,
        name
    FROM
        {{source('ethereum', 'ethereum_contracts')}}
    WHERE 1=1
        AND meta:decimals IS NOT NULL
),

--deposits from Aave LendingPool contract
deposits AS(
    SELECT
        DISTINCT block_id,
        block_timestamp,
        CASE
            WHEN COALESCE(event_inputs:reserve::string,event_inputs:_reserve::string) = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ELSE COALESCE(event_inputs:reserve::string,event_inputs:_reserve::string)
              END AS aave_market,
        COALESCE(event_inputs:amount,event_inputs:_amount) AS deposit_quantity, --not adjusted for decimals
        tx_from_address AS depositor_address,
        tx_to_address AS lending_pool_contract,
        tx_id,
        CASE
            WHEN contract_address = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN contract_address = LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') THEN 'Aave V1'
            WHEN contract_address = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
          ELSE 'ERROR' END AS aave_version
    FROM
        {{ref('ethereum__events_emitted')}} deposit
    WHERE 1=1
        AND block_timestamp::date >= '2021-01-01'
        AND contract_address IN(--Aave V2 LendingPool contract address
            LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'),--V2
            LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119'),--V1
            LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'))--AMM
        AND event_name = 'Deposit' --this is a deposit
        AND tx_succeeded = TRUE --excludes failed txs
)


SELECT
    deposits.tx_id,
    deposits.block_id,
    deposits.block_timestamp,
    LOWER(deposits.aave_market) AS aave_market,
    LOWER(underlying.aave_token) AS aave_token,
    deposits.deposit_quantity /
        POW(10,COALESCE(coalesced_prices.decimals,backup_prices.decimals,prices_daily_backup.decimals,decimals_backup.decimals,18)) AS issued_atokens,
    deposits.deposit_quantity * COALESCE(coalesced_prices.coalesced_price,backup_prices.price,prices_daily_backup.avg_daily_price) /
        POW(10,COALESCE(coalesced_prices.decimals,backup_prices.decimals,prices_daily_backup.decimals,decimals_backup.decimals,18)) AS supplied_usd,
    LOWER(deposits.depositor_address) AS depositor_address,
    LOWER(deposits.lending_pool_contract) AS lending_pool_contract,
    deposits.aave_version,
    COALESCE(coalesced_prices.coalesced_price,backup_prices.price,prices_daily_backup.avg_daily_price) AS token_price,
    COALESCE(coalesced_prices.symbol,backup_prices.symbol,prices_daily_backup.symbol) AS symbol
FROM
    deposits
    LEFT JOIN coalesced_prices
        ON LOWER(deposits.aave_market) = LOWER(coalesced_prices.token_contract)
        AND deposits.aave_version = coalesced_prices.aave_version
        AND date_trunc('hour',deposits.block_timestamp) = coalesced_prices.coalesced_hour
    LEFT JOIN backup_prices
        ON LOWER(deposits.aave_market) = LOWER(backup_prices.token_address)
        AND date_trunc('hour',deposits.block_timestamp) = backup_prices.hour
    LEFT JOIN prices_daily_backup
        ON LOWER(deposits.aave_market) = LOWER(prices_daily_backup.token_address)
        AND date_trunc('day',deposits.block_timestamp) = prices_daily_backup.block_date
    LEFT JOIN underlying
        ON LOWER(deposits.aave_market) = LOWER(underlying.token_contract)
        AND deposits.aave_version = underlying.aave_version
    LEFT JOIN decimals_backup
        ON LOWER(deposits.aave_market) = LOWER(decimals_backup.token_address)
