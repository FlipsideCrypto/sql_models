{{
  config(
    materialized='table',
    unique_key='block_hour || aave_version || underlying_contract || atoken_price || aave_price || reserve_price',
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'aave', 'aave_market_stats']
  )
}}

-- first grab the contract reads from the lending pool (V1,V2 and AMM) and data provider contracts (V2,AMM)
-- they are all as one concatonated string so we need to parse them out
WITH aave_reads AS (

    SELECT 
        DISTINCT
          date_trunc('hour',block_timestamp) AS blockhour,
          contract_address AS lending_pool_add,
          CASE 
            WHEN contract_address IN (LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d'),LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba')) THEN 'DataProvider'
            ELSE 'LendingPool' END AS
          lending_pool_type,
          function_name,
          CASE 
            WHEN contract_address IN (LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'),LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d')) THEN 'Aave V2'
            WHEN contract_address IN (LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba')) THEN 'Aave AMM'
            ELSE 'Aave V1'
    END AS aave_version, 
    inputs,
                (SPLIT(LOWER(VALUE_STR),'^')) AS coins
    FROM {{source('ethereum', 'ethereum_reads')}}
    WHERE 

        contract_address  IN (
           
            LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d'), -- AAVE V2 Data Provider (per docs)
            LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'), -- AAVE AMM Lending Pool (per docs)
            LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'), -- AAVE V2 Lending Pool (per docs)
            LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba'),  -- AAVE AMM Data Provider (per docs)
            LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') -- AAVE V1
) 
    AND block_timestamp >= CURRENT_DATE - 60
    
    -- first split them into a log format where we have one row per field per read
), long_format AS (
   SELECT 
    DISTINCT
      blockhour, 
      aave_version, 
      lending_pool_add,
      lending_pool_type,
      CASE WHEN LOWER(COALESCE(inputs:address,inputs:_reserve)) = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
           ELSE LOWER(COALESCE(inputs:address,inputs:_reserve)) END AS reserve_token,
      --inputs::STRING AS input_type,
      --inputs::STRING AS input_address,
      (SPLIT(LOWER(value::STRING),':'))[0] AS field_name,
      (SPLIT(LOWER(value::STRING),':'))[1] AS value
  FROM aave_reads, 
  Table(Flatten(aave_reads.coins))
  WHERE value::STRING <> '0x0000000000000000000000000000000000000000'-- AND inputs:address IS NULL
), 
    -- cast to a wide format next putting one column for every field in every read
wide_format AS (
    SELECT * 
    FROM long_format 
    pivot(max(value) for field_name 
          IN ('averagestableborrowrate', 'id', 'interestratestrategyaddress', 'stabledebttokenaddress', 
              'currentstableborrowrate', 'currentvariableborrowrate', 'totalliquidity', 'currentliquidityrate', 
              'variableborrowindex', 'variableborrowrate', 'atokenaddress', 'availableliquidity', 'version', 
              'liquidityrate', 'stableborrowrate', 'totalborrowsstable', 'totalborrowsvariable', 
              'lastupdatetimestamp', 'liquidityindex', 'utilizationrate', 
              'variabledebttokenaddress', 'totalstabledebt', 'totalvariabledebt'))

), 
-- combine redundant columns, decimal adjust rates (these all use the same adjustment)
reads_parsed AS (
   SELECT 
    DISTINCT
    blockhour,aave_version,lending_pool_add,lending_pool_type,reserve_token,
           COALESCE("'atokenaddress'",'-')::STRING AS atoken_address,
           COALESCE("'stabledebttokenaddress'",'-')::STRING   AS stable_debt_token_address,
           COALESCE("'variabledebttokenaddress'",'-')::STRING AS variable_debt_token_address,
           COALESCE("'availableliquidity'","'totalliquidity'",NULL)::NUMERIC AS available_liquidity,
           COALESCE("'currentliquidityrate'","'liquidityrate'",NULL)::NUMERIC/POWER(10,27) AS liquidity_rate,
           COALESCE("'averagestableborrowrate'","'currentstableborrowrate'",NULL)::NUMERIC/POWER(10,27) AS stbl_borrow_rate,
           COALESCE("'currentvariableborrowrate'","'variableborrowindex'","'variableborrowrate'",NULL)::NUMERIC/POWER(10,27) AS variable_borrow_rate,
           COALESCE("'totalstabledebt'","'totalborrowsstable'",NULL)::NUMERIC AS total_stable_debt,
           COALESCE("'totalvariabledebt'","'totalborrowsvariable'",NULL)::NUMERIC AS total_variable_debt,
           COALESCE("'utilizationrate'",NULL)::NUMERIC/POWER(10,27) AS utilization_rate
   FROM wide_format
   ORDER BY reserve_token, blockhour DESC

),
-- splitting these up for organization
lending_pools_v2 AS (
    SELECT * FROM reads_parsed WHERE lending_pool_type = 'LendingPool' AND aave_version <> 'V1'
), data_providers_v2 AS (
    SELECT * FROM reads_parsed WHERE lending_pool_type = 'DataProvider' AND aave_version <> 'V1'
), lending_pools_v1 AS (
  SELECT * FROM reads_parsed WHERE lending_pool_type = 'LendingPool' AND aave_version = 'V1'
), 
-- format v2/amm data. Need to combine reads from the lending pool and data provider
aave_v2 AS (
  
  SELECT 
    lp.blockhour,
    lp.reserve_token,
    lp.aave_version,
    lp.lending_pool_add,
    dp.lending_pool_add AS data_provider,
    lp.atoken_address,
    lp.stable_debt_token_address,
    lp.variable_debt_token_address,
    (dp.available_liquidity + dp.total_stable_debt + dp.total_variable_debt) AS total_liquidity,
    CASE WHEN lp.liquidity_rate IS NOT NULL THEN lp.liquidity_rate
         ELSE dp.liquidity_rate END AS liquidity_rate,
    CASE WHEN lp.stbl_borrow_rate IS NOT NULL THEN lp.stbl_borrow_rate 
         ELSE dp.stbl_borrow_rate END AS stbl_borrow_rate,
    CASE WHEN lp.variable_borrow_rate IS NOT NULL THEN lp.variable_borrow_rate 
         ELSE dp.variable_borrow_rate END AS variable_borrow_rate,
    dp.total_stable_debt AS total_stable_debt,
    dp.total_variable_debt AS total_variable_debt,
    CASE WHEN total_liquidity <> 0 THEN ((dp.total_stable_debt + dp.total_variable_debt)/total_liquidity) ELSE 0 END AS utilization_rate
  FROM 
  lending_pools_v2 lp
  LEFT OUTER JOIN 
  data_providers_v2 dp
  ON lp.reserve_token = dp.reserve_token and lp.blockhour = dp.blockhour and lp.aave_version = dp.aave_version
  ORDER BY blockhour DESC

), 
-- format v1 data
aave_v1 AS (
    SELECT 
    lp.blockhour,
    lp.reserve_token,
    lp.aave_version,
    lp.lending_pool_add,
    '-' AS data_provider,
    lp.atoken_address,
    lp.stable_debt_token_address,
    lp.variable_debt_token_address,
    lp.available_liquidity AS total_liquidity,
    lp.liquidity_rate,
    lp.stbl_borrow_rate,
    lp.variable_borrow_rate,
    lp.total_stable_debt,
    lp.total_variable_debt,
    lp.utilization_rate
  FROM 
  lending_pools_v1 lp
  ORDER BY blockhour DESC
), 
aave AS (
  
  SELECT * FROM aave_v2
  UNION
  SELECT * FROM aave_v1
  
),
-- get decimals, preferably from contract reads but using the prices table as a fallback
decimals_raw as (

  SELECT address AS token_address,
  meta:decimals AS decimals,name,
  2 as weight
  FROM {{source('ethereum', 'ethereum_contracts')}}
  WHERE meta:decimals IS NOT NULL

  UNION

  SELECT DISTINCT token_address,
  decimals,symbol AS name,
  1 AS weight
  FROM {{ref('ethereum__token_prices_hourly')}}
  WHERE 
   decimals IS NOT NULL

), decimals AS (
  SELECT token_address,decimals,name
  FROM decimals_raw
  QUALIFY (row_number() OVER (partition by token_address order by weight desc)) = 1
),
-- implementing aave oracle prices denominated in wei
oracle AS(
    SELECT
        date_trunc('hour',block_timestamp) AS hour,
        LOWER(inputs:address::string) AS token_address,
        MEDIAN(value_numeric) AS value_ethereum -- values are given in wei and need to be converted to ethereum
    FROM
        {{ref('ethereum__reads')}}
    WHERE 
        contract_address = '0xa50ba011c48153de246e5192c8f9258a2ba79ca9' -- check if there is only one oracle
        AND block_timestamp::date >= '2021-05-01'
  GROUP BY 1,2
),
--pull hourly prices for each underlying
aave_prices AS (
    SELECT
        o.hour,
        (o.value_ethereum * POWER(10,18)) * p.price AS price, -- this is all to get price in wei to price in USD
        o.token_address
    FROM
    oracle o
    INNER JOIN {{ref('ethereum__token_prices_hourly')}} p
      ON o.hour = p.hour
       AND p.hour::date >= '2021-05-01'
       AND p.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    
), 
-- calculate what we can
aave_data AS (
  SELECT 
          a.blockhour,
          a.reserve_token,
          --l.address_name AS reserve_name,
          COALESCE(p.symbol,REGEXP_REPLACE(l.address_name,'AAVE.*: a','')) AS reserve_name,
          a.aave_version,
          a.lending_pool_add,
          COALESCE(p.price,ap.price) AS reserve_price,
          a.data_provider,
          a.atoken_address,
          a.stable_debt_token_address,
          a.variable_debt_token_address,
          a.total_liquidity/POWER(10,d.decimals) AS total_liquidity_token,
          a.total_liquidity*COALESCE(p.price,ap.price)/POWER(10,d.decimals) AS total_liquidity_usd,
          a.liquidity_rate,
          a.stbl_borrow_rate,
          a.variable_borrow_rate,
          a.total_stable_debt/POWER(10,d.decimals) AS total_stable_debt_token,
          a.total_stable_debt*COALESCE(p.price,ap.price)/POWER(10,d.decimals) AS total_stable_debt_usd,
          a.total_variable_debt/POWER(10,d.decimals)AS total_variable_debt_token,
          a.total_variable_debt*COALESCE(p.price,ap.price)/POWER(10,d.decimals)AS total_variable_debt_usd,
          utilization_rate
  FROM aave a
  --LEFT OUTER JOIN
  --silver.ethereum_address_labels l ON a.reserve_token = l.address
  LEFT OUTER JOIN
  decimals d ON a.reserve_token = d.token_address
  LEFT OUTER JOIN
  {{source('ethereum', 'ethereum_address_labels')}} l ON a.atoken_address = l.address
  LEFT OUTER JOIN
  aave_prices ap ON a.reserve_token = ap.token_address AND a.blockhour = ap.hour
  LEFT OUTER JOIN
  {{ref('ethereum__token_prices_hourly')}} p ON a.reserve_token = p.token_address AND a.blockhour = p.hour
  ORDER BY reserve_token, blockhour DESC
  
)

--finally format to spec/with some adjustments
SELECT
    a.blockhour as block_hour,
    a.reserve_token AS aave_market,
    a.lending_pool_add, -- use these two for debugging reads, input the underlying token
    a.data_provider, --
    a.reserve_name,
    a.atoken_address,
    a.stable_debt_token_address,
    a.variable_debt_token_address,
    a.reserve_price,
    atok.price AS atoken_price,
    a.total_liquidity_token,
    a.total_liquidity_usd,
    a.total_stable_debt_token,
    a.total_stable_debt_usd,
    a.total_variable_debt_token,
    a.total_variable_debt_usd,
    a.liquidity_rate AS supply_rate,
    a.stbl_borrow_rate AS borrow_rate_stable,
    a.variable_borrow_rate AS borrow_rate_variable,
    aave.price AS aave_price,
    a.aave_version,
    'ethereum' AS blockchain
FROM 
aave_data a
LEFT OUTER JOIN
{{ref('ethereum__token_prices_hourly')}} atok
ON a.atoken_address = atok.token_address AND a.blockhour = atok.hour
LEFT OUTER JOIN
{{ref('ethereum__token_prices_hourly')}} aave
ON aave.token_address = LOWER('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9') AND a.blockhour = aave.hour
ORDER BY underlying_contract, blockhour DESC