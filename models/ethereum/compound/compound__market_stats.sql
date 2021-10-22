{{ 
  config(
    materialized='incremental', 
    sort='block_hour', 
    unique_key='block_hour', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'compound', 'compound_market_stats']
  )
}}


-- pull all ctoken addresses and corresponding name
WITH ctoks as (
  SELECT
      DISTINCT contract_address as address,
      CASE WHEN contract_address = '0x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e' THEN 'cBAT'
          WHEN contract_address = '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4' THEN 'cCOMP'
          WHEN contract_address = '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643' THEN 'cDAI'
          WHEN contract_address = '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5' THEN 'cETH'
          WHEN contract_address = '0x158079ee67fce2f58472a96584a73c7ab9ac95c1' THEN 'cREP'
          WHEN contract_address = '0xf5dce57282a584d2746faf1593d3121fcac444dc' THEN 'cSAI'
          WHEN contract_address = '0x35a18000230da775cac24873d00ff85bccded550' THEN 'cUNI'
          WHEN contract_address = '0x39aa39c021dfbae8fac545936693ac917d5e7563' THEN 'cUSDC'
          WHEN contract_address = '0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9' THEN 'cUSDT'
          WHEN contract_address = '0xc11b1268c1a384e55c48c2391d8d480264a3a7f4' THEN 'cWBTC'
          WHEN contract_address = '0xccf4429db6322d5c611ee964527d42e5d685dd6a' THEN 'cWBTC2'
          WHEN contract_address = '0xe65cdb6479bac1e22340e4e755fae7e509ecd06c' THEN 'cAAVE'
          WHEN contract_address = '0xface851a4921ce59e912d19329929ce6da6eb0c7' THEN 'cLINK'
          WHEN contract_address = '0x95b4ef2869ebd94beb4eee400a99824bf5dc325b' THEN 'cMKR'
          WHEN contract_address = '0x4b0181102a0112a2ef11abee5563bb4a3176c9d7' THEN 'cSUSHI'
          WHEN contract_address = '0x80a2ae356fc9ef4305676f7a3e2ed04e12c33946' THEN 'cYFI'
          WHEN contract_address = '0x12392f67bdf24fae0af363c24ac620a2f67dad86' THEN 'cTUSD'
          WHEN contract_address = '0xb3319f5d18bc0d84dd1b4825dcde5d5f7266d407' THEN 'cZRX' end project_name
      FROM {{ref('silver_ethereum__events_emitted')}}
      WHERE contract_address in (
      '0x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e', -- cbat
      '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4', -- ccomp
      '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643', -- cdai
      '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5', -- cETH
      '0x158079ee67fce2f58472a96584a73c7ab9ac95c1', -- cREP
      '0xf5dce57282a584d2746faf1593d3121fcac444dc', -- csai
      '0x35a18000230da775cac24873d00ff85bccded550', -- cuni
      '0x39aa39c021dfbae8fac545936693ac917d5e7563', -- cusdc
      '0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9', -- cusdt
      '0xc11b1268c1a384e55c48c2391d8d480264a3a7f4', -- cwbtc
      '0xccf4429db6322d5c611ee964527d42e5d685dd6a', -- cwbtc2
      '0xb3319f5d18bc0d84dd1b4825dcde5d5f7266d407', -- czrx
      '0xe65cdb6479bac1e22340e4e755fae7e509ecd06c', -- caave
      '0xface851a4921ce59e912d19329929ce6da6eb0c7', -- clink
      '0x95b4ef2869ebd94beb4eee400a99824bf5dc325b', -- cmkr
      '0x4b0181102a0112a2ef11abee5563bb4a3176c9d7', -- csushi
      '0x80a2ae356fc9ef4305676f7a3e2ed04e12c33946', -- cyfi
      '0x12392f67bdf24fae0af363c24ac620a2f67dad86' -- ctusd
      )
      AND block_timestamp > getdate() - interval '31 days'
),
ctok_decimals AS (
    SELECT DISTINCT 
        contract_address, 
        value_numeric AS decimals
    FROM {{ref('silver_ethereum__reads')}}
    WHERE 
        {% if is_incremental() %}
            block_timestamp >= getdate() - interval '2 days'
        {% else %}
            block_timestamp >= getdate() - interval '9 months'
        {% endif %}
        AND contract_address IN (SELECT address FROM ctoks)
        AND function_name = 'decimals'
),

-- look up underlying token
underlying AS (
  SELECT DISTINCT 
    contract_address as address, 
    LOWER(value_string) as token_contract
  FROM {{ref('silver_ethereum__reads')}}
  WHERE 
    contract_address IN (SELECT address FROM ctoks)
    AND function_name = 'underlying'
    {% if is_incremental() %}
        AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
        AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}
   
   UNION
   
   -- this grabs weth for the cETH contract
  SELECT 
    contract_address AS address, 
    LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token_contract
  FROM {{ref('silver_ethereum__reads')}}
  WHERE 
    contract_address = '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5'
    {% if is_incremental() %}
        AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
        AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}   
),

--pull hourly prices for each undelrying
prices AS (
    SELECT 
      hour as block_hour,
      price as token_price,
      decimals as token_decimals,
      pr.symbol,
      pr.token_address as token_contract, -- this is the undelrying asset
      underlying.address -- this is the ctoken
    FROM {{ref('ethereum__token_prices_hourly')}} AS pr
    INNER JOIN underlying 
      ON pr.token_address = underlying.token_contract
    WHERE     
      {% if is_incremental() %}
          hour >= getdate() - interval '2 days'
      {% else %}
          hour >= getdate() - interval '9 months'
      {% endif %}   
),
-- all the ingredients for the supply, borrows, and reserve market data
ingreds as (
    SELECT DISTINCT 
        date_trunc('hour',block_timestamp) as block_hour,
        contract_address as address,
        token_contract,
        contract_name,
        function_name,
        prices.token_price,
        prices.token_decimals,
        prices.symbol as underlying_symbol,
        last_value(value_numeric) OVER (PARTITION BY block_hour, address, contract_name, function_name ORDER BY block_timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as num
    FROM {{ref('silver_ethereum__reads')}} rds 
    INNER JOIN prices 
        ON date_trunc('hour',rds.block_timestamp) = prices.block_hour 
        AND rds.contract_address = prices.address
    WHERE 
      rds.contract_address IN (SELECT address FROM ctoks)
      AND function_name IN ('exchangeRateStored','totalReserves','totalBorrows','totalSupply','decimals')
      {% if is_incremental() %}
      AND block_timestamp >= getdate() - interval '2 days'
      {% else %}
      AND block_timestamp >= getdate() - interval '9 months'
      {% endif %}   
    order by 1 desc
),

-- market data with usd-equivalents based on prices and exchange rates
markets AS (
  SELECT 
    block_hour,
    contract_name,
    address as ctoken_address,
    token_contract as underlying_contract,
    underlying_symbol,
    token_price, 
    "'decimals'" as ctoken_decimals,
    token_decimals,
    ("'exchangeRateStored'" / pow(10, 18 + (token_decimals - ctoken_decimals))) * token_price as ctoken_price,
    "'totalReserves'" / pow(10,token_decimals) as reserves_token_amount,
    "'totalBorrows'" / pow(10,token_decimals) as borrows_token_amount,
    "'totalSupply'" / pow(10,ctoken_decimals) as supply_token_amount,
    supply_token_amount * ctoken_price as supply_usd,
    reserves_token_amount * token_price as reserves_usd,
    borrows_token_amount * token_price as borrows_usd
  FROM ingreds
    pivot(max(num) for function_name IN ('exchangeRateStored', 'totalReserves', 'totalBorrows', 'totalSupply', 'decimals')) 
      AS p
  ORDER BY block_hour DESC
),

-- comp emitted by ctoken by hour
comptr AS (
  SELECT DISTINCT 
    date_trunc('hour',erd.block_timestamp) as blockhour,
    LOWER(REGEXP_REPLACE(inputs:c_token_address,'\"','')) as ctoken_address,
    sum(erd.value_numeric / 1e18) as comp_speed,
    p.token_price as comp_price,
    comp_price * comp_speed as comp_speed_usd
  FROM {{ref('silver_ethereum__reads')}} erd 
  JOIN (SELECT * FROM prices WHERE symbol = 'COMP') p
    ON date_trunc('hour',erd.block_timestamp) = p.block_hour
  WHERE 
    contract_address = '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b'
    AND function_name = 'compSpeeds'
    {% if is_incremental() %}
        AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
        AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}    
  GROUP BY 1,2,4
),
-- supply APY
supply AS (

  SELECT 
    DATE_TRUNC('hour',block_timestamp) AS blockhour,
    contract_address AS ctoken_address,
    (((POWER(AVG(value_numeric) / 1e18 * ((60/13.15) * 60 * 24) + 1,365))) - 1) AS apy
  FROM {{ref('silver_ethereum__reads')}}
  WHERE function_name = 'supplyRatePerBlock' 
    AND project_name = 'compound'
    {% if is_incremental() %}
        AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
        AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}    
  GROUP BY 1,2

),
-- borrow APY
borrow AS (

  SELECT 
    DATE_TRUNC('hour',block_timestamp) AS blockhour,
    contract_address AS ctoken_address,
    (((POWER(AVG(value_numeric) / 1e18 * ((60/13.15) * 60 * 24) + 1,365))) - 1) AS apy
  FROM {{ref('silver_ethereum__reads')}}
  WHERE function_name = 'borrowRatePerBlock' 
    AND project_name = 'compound'
    {% if is_incremental() %}
        AND block_timestamp >= getdate() - interval '2 days'
    {% else %}
        AND block_timestamp >= getdate() - interval '9 months'
    {% endif %}    
  GROUP BY 1,2

)

SELECT 
  a.block_hour,
  a.contract_name,
  a.ctoken_address,
  CASE WHEN a.underlying_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN NULL ELSE a.underlying_contract END AS underlying_contract,
  CASE WHEN a.underlying_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'ETH' ELSE a.underlying_symbol END AS underlying_symbol,
  a.token_price,
  --a.ctoken_decimals,
  --a.token_decimals,
  a.ctoken_price,
  a.reserves_token_amount,
  a.borrows_token_amount,
  a.supply_token_amount,
  a.supply_usd,
  a.reserves_usd,
  a.borrows_usd,
  b.comp_speed,
  supply.apy AS supply_apy,
  borrow.apy AS borrow_apy,
  b.comp_price,
  b.comp_speed_usd,
  case 
    when borrows_usd != 0
    then POWER((1 + ((b.comp_speed_usd * 24) / borrows_usd)),365)-1
    else null
  end as comp_apy_borrow,
  case 
    when supply_usd != 0
    then POWER((1 + ((b.comp_speed_usd * 24) / supply_usd)),365)-1
    else null
  end as comp_apy_supply
FROM markets a 
JOIN comptr b 
  ON a.ctoken_address = b.ctoken_address AND a.block_hour = b.blockhour
LEFT JOIN supply
  ON a.ctoken_address = supply.ctoken_address AND a.block_hour = supply.blockhour
LEFT JOIN borrow
  ON a.ctoken_address = borrow.ctoken_address AND a.block_hour = borrow.blockhour
ORDER BY block_hour DESC, a.contract_name