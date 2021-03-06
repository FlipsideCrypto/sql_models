{{ 
  config(
    materialized='incremental', 
    sort='block_id', 
    unique_key='block_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'compound', 'compound_liquidations']
  )
}}


-- pull all ctoken addresses and corresponding name
WITH ctoks as (
  SELECT
      DISTINCT contract_addr as address,
      CASE WHEN contract_addr = '0x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e' THEN 'cBAT'
          WHEN contract_addr = '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4' THEN 'cCOMP'
          WHEN contract_addr = '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643' THEN 'cDAI'
          WHEN contract_addr = '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5' THEN 'cETH'
          WHEN contract_addr = '0x158079ee67fce2f58472a96584a73c7ab9ac95c1' THEN 'cREP'
          WHEN contract_addr = '0xf5dce57282a584d2746faf1593d3121fcac444dc' THEN 'cSAI'
          WHEN contract_addr = '0x35a18000230da775cac24873d00ff85bccded550' THEN 'cUNI'
          WHEN contract_addr = '0x39aa39c021dfbae8fac545936693ac917d5e7563' THEN 'cUSDC'
          WHEN contract_addr = '0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9' THEN 'cUSDT'
          WHEN contract_addr = '0xc11b1268c1a384e55c48c2391d8d480264a3a7f4' THEN 'cWBTC'
          WHEN contract_addr = '0xccf4429db6322d5c611ee964527d42e5d685dd6a' THEN 'cWBTC2'
          WHEN contract_addr = '0xe65cdb6479bac1e22340e4e755fae7e509ecd06c' THEN 'cAAVE'
          WHEN contract_addr = '0xface851a4921ce59e912d19329929ce6da6eb0c7' THEN 'cLINK'
          WHEN contract_addr = '0x95b4ef2869ebd94beb4eee400a99824bf5dc325b' THEN 'cMKR'
          WHEN contract_addr = '0x4b0181102a0112a2ef11abee5563bb4a3176c9d7' THEN 'cSUSHI'
          WHEN contract_addr = '0x80a2ae356fc9ef4305676f7a3e2ed04e12c33946' THEN 'cYFI'
          WHEN contract_addr = '0x12392f67bdf24fae0af363c24ac620a2f67dad86' THEN 'cTUSD'
          WHEN contract_addr = '0xb3319f5d18bc0d84dd1b4825dcde5d5f7266d407' THEN 'cZRX' end project_name
      FROM {{ref('silver_ethereum__events_emitted')}}
      WHERE contract_addr in (
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
)
SELECT 
  DISTINCT block_id,
    block_timestamp,
    REGEXP_REPLACE(event_inputs:borrower,'\"','') AS borrower,
    ee.contract_addr AS ctoken, 
    ctoks.project_name AS ctoken_symbol,
    REGEXP_REPLACE(event_inputs:liquidator,'\"','') AS liquidator,
    event_inputs:seizeTokens/pow(10,d.decimals) AS ctokens_seized,
    event_inputs:repayAmount*p.token_price/pow(10,p.token_decimals) AS liquidation_amount,
    event_inputs:repayAmount*p.token_price/pow(10,p.token_decimals) AS liquidation_amount_usd,
    CASE WHEN p.token_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN NULL ELSE p.token_contract END AS liquidation_contract_address,
    CASE WHEN p.token_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'ETH' ELSE p.symbol END AS liquidation_contract_symbol,
    tx_id
FROM {{ ref('silver_ethereum__events_emitted') }} ee 
LEFT JOIN
prices p
ON date_trunc('hour',ee.block_timestamp) = p.block_hour 
    AND ee.contract_addr = p.address
LEFT OUTER JOIN
ctoks 
    ON ee.contract_addr = ctoks.address
LEFT JOIN
ctok_decimals d
    ON ee.contract_addr = d.contract_address
WHERE 
    {% if is_incremental() %}
    block_timestamp >= getdate() - interval '2 days'
    {% else %}
    block_timestamp >= getdate() - interval '9 months'
    {% endif %}   
    AND ee.contract_addr IN (select address from ctoks)
    AND ee.event_name = 'LiquidateBorrow'

