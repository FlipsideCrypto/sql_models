{{ 
  config(
    materialized='incremental', 
    sort='pool_address', 
    unique_key='pool_address || factory', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'dex','dex_liquidity_pools', 'curve_liquidity_pools']
  )
}}
WITH pool_tokens AS (
    SELECT 
      DISTINCT
          contract_address AS factory,
          LOWER(inputs:_pool::STRING) AS pool_add, 
          (SPLIT(LOWER(VALUE_STRING),'^')) AS coins
    FROM {{ref('silver_ethereum__reads')}}
    WHERE 
      contract_name='Vyper_contract' 
      AND contract_address IN('0x0959158b6040d32d04c301a72cbfd6b39e21c9ae',
                          LOWER('0xfD6f33A0509ec67dEFc500755322aBd9Df1bD5B8'), 
                              '0x90e00ace148ca3b23ac1bc8c240c2a7dd9c2d7f5','0x7D86446dDb609eD0F5f8684AcF30380a356b2B4c')  
      AND function_name = 'get_underlying_coins'
      AND block_timestamp >= CURRENT_DATE - 60

), parsed AS (
  SELECT 
    DISTINCT
        pool_add, 
        factory,
        value::STRING AS coins, 
          (row_number() OVER (partition by pool_add order by pool_add desc) - 1 ) AS index
    FROM pool_tokens, 
    Table(Flatten(pool_tokens.coins))
    WHERE 
      value::STRING <> '0x0000000000000000000000000000000000000000'

), labeled_parsed AS (
    SELECT 
            p.pool_add,
            p.factory,
            CASE WHEN p.coins = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' ELSE p.coins END AS token,
            COALESCE(l.meta:symbol,l.name,token) AS name 
    FROM
    parsed p
    LEFT JOIN {{ref('silver_ethereum__contracts')}} l
        ON (CASE WHEN p.coins = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' ELSE p.coins END) = l.address

), names_final AS (
    SELECT 
        factory,
        pool_add AS pool_address,
        LISTAGG(name,'-') || ' Curve LP' AS pool_name, 
        ARRAYAGG(token) AS tokens
    FROM labeled_parsed
      GROUP BY 1,2

)
  
SELECT 
    n.factory,
    n.pool_address,
    COALESCE(l.address,n.pool_name) AS pool_name,
    n.tokens
FROM names_final n
LEFT JOIN {{ref('silver_crosschain__address_labels')}} l
    ON n.pool_address = l.address AND l.blockchain = 'ethereum' AND l.creator = 'flipside'
    QUALIFY (row_number() OVER (partition by pool_name order by factory desc)) = 1