{{ config(
  materialized = 'incremental',
  sort = 'creation_time',
  unique_key = 'pool_address',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'dex','dex_liquidity_pools', 'address_labels']
) }}

WITH v3_pools AS (
  -- uni v3

  SELECT
    block_timestamp AS creation_time,
    tx_id AS creation_tx,
    factory_address,
    REGEXP_REPLACE(
      pool_name,
      '$',
      ' UNI-V3 LP'
    ) AS pool_name,
    pool_address,
    token0,
    token1,
    'uniswap-v3' AS platform
  FROM
    {{ ref('silver_uniswapv3__pools') }}
  WHERE

{% if is_incremental() %}
creation_time >= getdate() - INTERVAL '7 days'
{% else %}
  creation_time >= '2021-01-05'
{% endif %}
),
asset_metadata_deduped AS (
  SELECT
    LOWER(token_address) AS token_address,
    symbol
  FROM
    {{ source(
      'shared',
      'market_asset_metadata'
    ) }}
    qualify(ROW_NUMBER() over (PARTITION BY LOWER(token_address)
  ORDER BY
    platform_id)) = 1),
    v2_pools AS (
      -- uni v2 and sushiswap
      SELECT
        p.block_timestamp AS creation_time,
        p.tx_id AS creation_tx,
        p.contract_addr AS factory_address,
        -- assign a liquidity pool name based on the two tokens in the pool in the format 'Token1-Token2 LP', i.e. 'WETH-DAI LP'
        -- goes through a couple fallbacks,
        ---- try ethereum_contracts
        ---- if the above is null, try market_asset_metadata
        ---- if the above is null, try to at least get a name instead of a symbol from ethereum_address_labels
        ---- if all else fails then just use the token contract address to yield an informative name
        COALESCE(
          A.meta :symbol,
          aa.symbol,
          aaa.address,
          p.event_inputs :token0 :: STRING
        ) || '-' || COALESCE(
          b.meta :symbol,
          bb.symbol,
          bbb.address,
          p.event_inputs :token1 :: STRING
        ) || ' LP' AS pool_name,
        p.event_inputs :pair :: STRING AS pool_address,
        p.event_inputs :token0 :: STRING AS token0,
        p.event_inputs :token1 :: STRING AS token1,
        CASE
          WHEN factory_address = '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac' THEN 'sushiswap'
          ELSE 'uniswap-v2'
        END AS platform
      FROM
        {{ ref('silver_ethereum__events_emitted') }}
        p
        LEFT JOIN {{ ref('silver_ethereum__contracts') }} A
        ON p.event_inputs :token0 :: STRING = A.address
        LEFT JOIN asset_metadata_deduped aa
        ON LOWER(
          p.event_inputs :token0 :: STRING
        ) = LOWER(
          aa.token_address
        )
        LEFT JOIN {{ ref('silver_crosschain__address_labels') }}
        aaa
        ON p.event_inputs :token0 :: STRING = aaa.address
        AND aaa.blockchain = 'ethereum'
        AND aaa.creator = 'flipside'
        LEFT JOIN {{ ref('silver_ethereum__contracts') }}
        b
        ON p.event_inputs :token1 :: STRING = b.address
        LEFT JOIN asset_metadata_deduped bb
        ON LOWER(
          p.event_inputs :token1 :: STRING
        ) = LOWER(
          bb.token_address
        )
        LEFT JOIN {{ ref('silver_crosschain__address_labels') }}
        bbb
        ON p.event_inputs :token1 :: STRING = bbb.address
        AND bbb.blockchain = 'ethereum'
        AND bbb.creator = 'flipside'
      WHERE
        p.event_name = 'PairCreated'

{% if is_incremental() %}
AND creation_time >= getdate() - INTERVAL '7 days'
{% else %}
  AND creation_time >= getdate() - INTERVAL '12 months'
{% endif %}
),
v2_redshift AS (
  SELECT
    p.block_timestamp AS creation_time,
    p.transaction_hash AS creation_tx,
    p.contract_address AS factory_address,
    -- assign a liquidity pool name based on the two tokens in the pool in the format 'Token1-Token2 LP', i.e. 'WETH-DAI LP'
    -- goes through a couple fallbacks,
    ---- try ethereum_contracts
    ---- if the above is null, try cmc_assets
    ---- if the above is null, try to at least get a name instead of a symbol from ethereum_address_labels
    ---- if all else fails then just use the token contract address to yield an informative name
    COALESCE(
      A.meta :symbol,
      aa.symbol,
      aaa.address,
      p.token0
    ) || '-' || COALESCE(
      b.meta :symbol,
      bb.symbol,
      bbb.address,
      p.token1
    ) || ' LP' AS pool_name,
    pair AS pool_address,
    token0,
    token1,
    CASE
      WHEN factory_address = '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac' THEN 'sushiswap'
      ELSE 'uniswap-v2'
    END AS platform
  FROM
    {{ source(
      'shared',
      'uniswapv2factory_event_paircreated'
    ) }}
    p -- {{ref('ethereum__events_emitted')}} p
    LEFT JOIN {{ ref('silver_ethereum__contracts') }} A
    ON token0 = A.address
    LEFT JOIN asset_metadata_deduped aa
    ON LOWER(token0) = LOWER(
      aa.token_address
    )
    LEFT JOIN {{ ref('silver_crosschain__address_labels') }}
    aaa
    ON token0 = aaa.address
    AND aaa.blockchain = 'ethereum'
    AND aaa.creator = 'flipside'
    LEFT JOIN {{ ref('silver_ethereum__contracts') }}
    b
    ON token1 = b.address
    LEFT JOIN asset_metadata_deduped bb
    ON LOWER(token1) = LOWER(
      bb.token_address
    )
    LEFT JOIN {{ ref('silver_crosschain__address_labels') }}
    bbb
    ON token1 = bbb.address
    AND bbb.blockchain = 'ethereum'
    AND bbb.creator = 'flipside'
),
sushi_write_in AS (
  -- adding a few major sushi pools that were created before we have eth data (this gives us data on swaps with these pools)
  -- edit now uses a table of sushiswap tables
  -- only captures the top 1000 pools by liquidity and pulls these from the Graph endpoint used by sushi https://api.thegraph.com/subgraphs/name/zippoxer/sushiswap-subgraph-fork
  SELECT
    DISTINCT NULL AS creation_time,
    NULL AS creation_tx,
    factory_address,
    CASE
      WHEN pool_name IS NULL
      AND platform = 'sushiswap' THEN token0 || '-' || token1 || ' SLP'
      WHEN pool_name IS NULL
      AND platform = 'uniswap-v2' THEN token0 || '-' || token1 || ' UNI-V2 LP'
      ELSE pool_name
    END AS pool_name,
    pool_address,
    token0,
    token1,
    platform
  FROM
    {{ ref('silver__historic_dex_pools') }}
),
new_sushi AS (
  SELECT
    s.* -- future proofing: once the eth backfill is done these manual write-ins will be dups
  FROM
    sushi_write_in s
    LEFT OUTER JOIN v2_pools v
    ON s.pool_address = v.pool_address
    LEFT OUTER JOIN v2_redshift r
    ON s.pool_address = r.pool_address
  WHERE
    v.pool_address IS NULL
    AND r.pool_address IS NULL
),
stack AS (
  -- get pool info
  SELECT
    *
  FROM
    v2_pools
  UNION
  SELECT
    *
  FROM
    v2_redshift
  UNION
  SELECT
    *
  FROM
    new_sushi
  UNION
  SELECT
    *
  FROM
    v3_pools
),
curve AS (
  SELECT
    *,
    ARRAY_CONSTRUCT(
      token0,
      token1
    ) AS tokens
  FROM
    stack
  WHERE
    pool_address IS NOT NULL
    AND token0 IS NOT NULL
    AND token1 IS NOT NULL
  UNION
  SELECT
    NULL :: STRING AS creation_time,
    NULL :: STRING AS creation_tx,
    factory AS factory_address,
    pool_name,
    pool_address,
    NULL AS token0,
    NULL AS token1,
    'curve' AS platform,
    tokens
  FROM
    {{ ref('ethereum_dbt__curve_liquidity_pools') }}
)
SELECT
  DISTINCT *
FROM
  curve
WHERE
  pool_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
  tokens DESC)) = 1
