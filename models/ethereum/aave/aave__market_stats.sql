{{ config(
  materialized = 'incremental',
  unique_key = 'block_hour || aave_version || aave_market',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'aave', 'aave_market_stats', 'address_labels', 'atb_test']
) }}
-- first grab the contract reads from the lending pool (V1,V2 and AMM) and data provider contracts (V2,AMM)
-- they are all as one concatonated string so we need to parse them out
WITH aave_reads AS (

  SELECT
    DISTINCT DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS blockhour,
    contract_address AS lending_pool_add,
    CASE
      WHEN contract_address IN (
        LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d'),
        LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba')
      ) THEN 'DataProvider'
      ELSE 'LendingPool'
    END AS lending_pool_type,
    function_name,
    CASE
      WHEN contract_address IN (
        LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'),
        LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d')
      ) THEN 'Aave V2'
      WHEN contract_address IN (
        LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
        LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba')
      ) THEN 'Aave AMM'
      ELSE 'Aave V1'
    END AS aave_version,
    inputs,
    (SPLIT(LOWER(value_string), '^')) AS coins
  FROM
    {{ ref('silver_ethereum__reads') }}
  WHERE
    contract_address IN (
      LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d'),
      -- AAVE V2 Data Provider (per docs)
      LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
      -- AAVE AMM Lending Pool (per docs)
      LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'),
      -- AAVE V2 Lending Pool (per docs)
      LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba'),
      -- AAVE AMM Data Provider (per docs)
      LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') -- AAVE V1
    )

{% if is_incremental() %}
AND block_timestamp :: DATE >= CURRENT_DATE - 2
{% else %}
  AND block_timestamp :: DATE >= CURRENT_DATE - 720
{% endif %}

-- first split them into a log format where we have one row per field per read
),
long_format AS (
  SELECT
    DISTINCT blockhour,
    aave_version,
    lending_pool_add,
    lending_pool_type,
    CASE
      WHEN LOWER(COALESCE(inputs :address, inputs :_reserve)) = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
      ELSE LOWER(COALESCE(inputs :address, inputs :_reserve))
    END AS reserve_token,
    --inputs::STRING AS input_type,
    --inputs::STRING AS input_address,
    (SPLIT(LOWER(VALUE :: STRING), ':')) [0] AS field_name,
    (SPLIT(LOWER(VALUE :: STRING), ':')) [1] AS VALUE
  FROM
    aave_reads,
    TABLE(FLATTEN(aave_reads.coins))
  WHERE
    VALUE :: STRING <> '0x0000000000000000000000000000000000000000' -- AND inputs:address IS NULL
),
-- cast to a wide format next putting one column for every field in every read
wide_format AS (
  SELECT
    *
  FROM
    long_format pivot(MAX(VALUE) for field_name IN ('averagestableborrowrate', 'id', 'interestratestrategyaddress', 'stabledebttokenaddress', 'currentstableborrowrate', 'currentvariableborrowrate', 'totalliquidity', 'currentliquidityrate', 'variableborrowindex', 'variableborrowrate', 'atokenaddress', 'availableliquidity', 'version', 'liquidityrate', 'stableborrowrate', 'totalborrowsstable', 'totalborrowsvariable', 'lastupdatetimestamp', 'liquidityindex', 'utilizationrate', 'variabledebttokenaddress', 'totalstabledebt', 'totalvariabledebt'))
),
-- combine redundant columns, decimal adjust rates (these all use the same adjustment)
reads_parsed AS (
  SELECT
    DISTINCT blockhour,
    aave_version,
    lending_pool_add,
    lending_pool_type,
    reserve_token,
    COALESCE(
      "'atokenaddress'",
      '-'
    ) :: STRING AS atoken_address,
    COALESCE(
      "'stabledebttokenaddress'",
      '-'
    ) :: STRING AS stable_debt_token_address,
    COALESCE(
      "'variabledebttokenaddress'",
      '-'
    ) :: STRING AS variable_debt_token_address,
    COALESCE(
      "'totalliquidity'",
      "'availableliquidity'",
      NULL
    ) :: numeric AS available_liquidity,
    COALESCE(
      "'currentliquidityrate'",
      "'liquidityrate'",
      NULL
    ) :: numeric / power(
      10,
      27
    ) AS liquidity_rate,
    COALESCE(
      "'averagestableborrowrate'",
      "'currentstableborrowrate'",
      NULL
    ) :: numeric / power(
      10,
      27
    ) AS stbl_borrow_rate,
    COALESCE(
      "'currentvariableborrowrate'",
      "'variableborrowindex'",
      "'variableborrowrate'",
      NULL
    ) :: numeric / power(
      10,
      27
    ) AS variable_borrow_rate,
    COALESCE(
      "'totalstabledebt'",
      "'totalborrowsstable'",
      NULL
    ) :: numeric AS total_stable_debt,
    COALESCE(
      "'totalvariabledebt'",
      "'totalborrowsvariable'",
      NULL
    ) :: numeric AS total_variable_debt,
    COALESCE(
      "'utilizationrate'",
      NULL
    ) :: numeric / power(
      10,
      27
    ) AS utilization_rate
  FROM
    wide_format
  ORDER BY
    reserve_token,
    blockhour DESC
),
-- splitting these up for organization
lending_pools_v2 AS (
  SELECT
    *
  FROM
    reads_parsed
  WHERE
    lending_pool_type = 'LendingPool'
    AND aave_version <> 'Aave V1'
),
data_providers_v2 AS (
  SELECT
    *
  FROM
    reads_parsed
  WHERE
    lending_pool_type = 'DataProvider'
    AND aave_version <> 'Aave V1'
),
lending_pools_v1 AS (
  SELECT
    *
  FROM
    reads_parsed
  WHERE
    lending_pool_type = 'LendingPool'
    AND aave_version = 'Aave V1'
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
    (
      dp.available_liquidity + dp.total_stable_debt + dp.total_variable_debt
    ) AS total_liquidity,
    CASE
      WHEN lp.liquidity_rate IS NOT NULL THEN lp.liquidity_rate
      ELSE dp.liquidity_rate
    END AS liquidity_rate,
    CASE
      WHEN lp.stbl_borrow_rate IS NOT NULL THEN lp.stbl_borrow_rate
      ELSE dp.stbl_borrow_rate
    END AS stbl_borrow_rate,
    CASE
      WHEN lp.variable_borrow_rate IS NOT NULL THEN lp.variable_borrow_rate
      ELSE dp.variable_borrow_rate
    END AS variable_borrow_rate,
    dp.total_stable_debt AS total_stable_debt,
    dp.total_variable_debt AS total_variable_debt,
    CASE
      WHEN total_liquidity <> 0 THEN ((dp.total_stable_debt + dp.total_variable_debt) / total_liquidity)
      ELSE 0
    END AS utilization_rate
  FROM
    lending_pools_v2 lp
    LEFT OUTER JOIN data_providers_v2 dp
    ON lp.reserve_token = dp.reserve_token
    AND lp.blockhour = dp.blockhour
    AND lp.aave_version = dp.aave_version
  ORDER BY
    blockhour DESC
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
  ORDER BY
    blockhour DESC
),
aave AS (
  SELECT
    *
  FROM
    aave_v2
  UNION
  SELECT
    *
  FROM
    aave_v1
),
-- get decimals, preferably from contract reads but using the prices table as a fallback
decimals_raw AS (
  SELECT
    address AS token_address,
    meta :decimals AS decimals,
    NAME,
    2 AS weight
  FROM
    {{ ref('silver_ethereum__contracts') }}
  WHERE
    meta :decimals IS NOT NULL
  UNION
  SELECT
    DISTINCT token_address,
    decimals,
    symbol AS NAME,
    1 AS weight
  FROM
    {{ ref('ethereum__token_prices_hourly') }}
  WHERE
    decimals IS NOT NULL
),
decimals AS (
  SELECT
    token_address,
    decimals,
    NAME
  FROM
    decimals_raw qualify (ROW_NUMBER() over (PARTITION BY token_address
  ORDER BY
    weight DESC)) = 1
),
-- implementing aave oracle prices denominated in wei
ORACLE AS(
  SELECT
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS HOUR,
    LOWER(
      inputs :address :: STRING
    ) AS token_address,
    MEDIAN(value_numeric) AS value_ethereum -- values are given in wei and need to be converted to ethereum
  FROM
    {{ ref('silver_ethereum__reads') }}
  WHERE
    contract_address = '0xa50ba011c48153de246e5192c8f9258a2ba79ca9' -- check if there is only one oracle

{% if is_incremental() %}
AND block_timestamp :: DATE >= CURRENT_DATE - 2
{% else %}
  AND block_timestamp :: DATE >= CURRENT_DATE - 720
{% endif %}
GROUP BY
  1,
  2
),
--pull hourly prices for each underlying
aave_prices AS (
  SELECT
    o.hour,
    (o.value_ethereum / pow(10,(18 - dc.decimals))) * p.price AS price,
    -- this is all to get price in wei to price in USD
    o.token_address
  FROM
    ORACLE o
    INNER JOIN {{ ref('ethereum__token_prices_hourly') }}
    p
    ON o.hour = p.hour

{% if is_incremental() %}
AND o.hour >= CURRENT_DATE - 2
{% else %}
  AND o.hour >= CURRENT_DATE - 720
{% endif %}
AND p.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
LEFT JOIN decimals_raw dc
ON o.token_address = dc.token_address
),
sym_matches AS (
  SELECT
    token_address,
    symbol,
    MIN(HOUR) AS first_appears
  FROM
    ethereum.token_prices_hourly
  WHERE
    HOUR >= CURRENT_DATE - 720
  GROUP BY
    1,
    2 qualify (ROW_NUMBER() over (PARTITION BY token_address
  ORDER BY
    first_appears DESC)) = 1
),
deduped_cmc_prices AS (
  SELECT
    p.token_address,
    p.hour,
    decimals,
    CASE
      WHEN p.symbol = 'KNCL' THEN 'KNC'
      WHEN p.symbol = 'AENJ' THEN 'aENJ'
      WHEN p.symbol = 'AETH' THEN 'aETH'
      WHEN p.symbol = 'REPv2' THEN 'REP'
      ELSE p.symbol
    END AS symbol,
    AVG(price) AS price -- table has duplicated rows for KNC / KNCL so we need to do a trick
  FROM
    {{ ref('ethereum__token_prices_hourly') }}
    p
    INNER JOIN sym_matches s
    ON p.token_address = s.token_address
    AND p.symbol = s.symbol
  WHERE
    1 = 1

{% if is_incremental() %}
AND HOUR >= CURRENT_DATE - 2
{% else %}
  AND HOUR >= CURRENT_DATE - 720
{% endif %}
GROUP BY
  1,
  2,
  3,
  4
),
-- calculate what we can
aave_data AS (
  SELECT
    DISTINCT A.blockhour,
    A.reserve_token,
    --l.address_name AS reserve_name,
    COALESCE(
      p.symbol,
      REGEXP_REPLACE(
        l.contract_addr,
        'AAVE.*: a',
        ''
      )
    ) AS reserve_name,
    A.aave_version,
    A.lending_pool_add,
    COALESCE(
      p.price,
      ap.price
    ) AS reserve_price,
    A.data_provider,
    A.atoken_address,
    A.stable_debt_token_address,
    A.variable_debt_token_address,
    A.total_liquidity / power(
      10,
      d.decimals
    ) AS total_liquidity_token,
    A.total_liquidity * COALESCE(
      p.price,
      ap.price
    ) / power(
      10,
      d.decimals
    ) AS total_liquidity_usd,
    A.liquidity_rate,
    A.stbl_borrow_rate,
    A.variable_borrow_rate,
    A.total_stable_debt / power(
      10,
      d.decimals
    ) AS total_stable_debt_token,
    A.total_stable_debt * COALESCE(
      p.price,
      ap.price
    ) / power(
      10,
      d.decimals
    ) AS total_stable_debt_usd,
    A.total_variable_debt / power(
      10,
      d.decimals
    ) AS total_variable_debt_token,
    A.total_variable_debt * COALESCE(
      p.price,
      ap.price
    ) / power(
      10,
      d.decimals
    ) AS total_variable_debt_usd,
    utilization_rate
  FROM
    aave A --LEFT OUTER JOIN
    --silver.ethereum_address_labels l ON a.reserve_token = l.address
    LEFT OUTER JOIN decimals d
    ON A.reserve_token = d.token_address
    LEFT OUTER JOIN {{ ref('silver_ethereum__events_emitted') }}
    l
    ON A.atoken_address = l.contract_addr
    LEFT OUTER JOIN aave_prices ap
    ON A.reserve_token = ap.token_address
    AND A.blockhour = ap.hour
    LEFT OUTER JOIN deduped_cmc_prices p
    ON A.reserve_token = p.token_address
    AND A.blockhour = p.hour
  ORDER BY
    reserve_token,
    blockhour DESC
),
emissions AS (
  SELECT
    DATE_TRUNC(
      'day',
      blockhour
    ) AS blockday,
    token_address,
    AVG(emissionpersecond) AS emissionpersecond
  FROM
    {{ ref('silver__aave_liquidity_mining') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND blockhour :: DATE >= CURRENT_DATE - 2
{% else %}
  AND blockhour :: DATE >= CURRENT_DATE - 720
{% endif %}
GROUP BY
  blockday,
  token_address
) --finally format to spec/with some adjustments
SELECT
  DISTINCT A.blockhour AS block_hour,
  UPPER(
    A.reserve_token
  ) AS aave_market,
  --uses labels as a fallback, some of which have mixed case
  A.lending_pool_add,
  -- use these two for debugging reads, input the underlying token
  A.data_provider,
  --
  A.reserve_name,
  A.atoken_address,
  A.stable_debt_token_address,
  A.variable_debt_token_address,
  A.reserve_price,
  atok.price AS atoken_price,
  A.total_liquidity_token,
  A.total_liquidity_usd,
  A.total_stable_debt_token,
  A.total_stable_debt_usd,
  A.total_variable_debt_token,
  A.total_variable_debt_usd,
  A.liquidity_rate AS supply_rate,
  A.stbl_borrow_rate AS borrow_rate_stable,
  A.variable_borrow_rate AS borrow_rate_variable,
  aave.price AS aave_price,
  A.utilization_rate,
  A.aave_version,
  'ethereum' AS blockchain,
  ((stable.emissionpersecond * aave.price * 31536000) / A.total_liquidity_usd) AS stkaave_rate_supply,
  ((borrow.emissionpersecond * aave.price * 31536000) / A.total_liquidity_usd) AS stkaave_rate_variable_borrow
FROM
  aave_data A
  LEFT OUTER JOIN deduped_cmc_prices atok
  ON A.atoken_address = atok.token_address
  AND A.blockhour = atok.hour
  LEFT OUTER JOIN deduped_cmc_prices aave
  ON aave.token_address = LOWER('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
  AND A.blockhour = aave.hour
  LEFT OUTER JOIN emissions stable
  ON LOWER(
    A.atoken_address
  ) = LOWER(
    stable.token_address
  )
  AND DATE_TRUNC(
    'day',
    A.blockhour
  ) = stable.blockday
  LEFT OUTER JOIN emissions borrow
  ON LOWER(
    A.variable_debt_token_address
  ) = LOWER(
    borrow.token_address
  )
  AND DATE_TRUNC(
    'day',
    A.blockhour
  ) = borrow.blockday
ORDER BY
  aave_market,
  block_hour DESC
