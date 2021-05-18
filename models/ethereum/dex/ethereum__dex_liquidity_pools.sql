{{ 
  config(
    materialized='incremental', 
    sort='creation_time', 
    unique_key='creation_tx', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'dex']
  )
}}
WITH v3_pools AS ( -- uni v3
      SELECT 
          block_timestamp AS creation_time,
          tx_id AS creation_tx,
          factory_address,
          REGEXP_REPLACE(pool_name,'$',' UNI-V3 LP') AS pool_name,
          pool_address,
          token0,
          token1,
          'uniswap-v3' AS platform
      FROM 
      {{source('uniswapv3_eth','uniswapv3_pools')}}
      WHERE 
      {% if is_incremental() %}
        creation_time >= getdate() - interval '2 days'
      {% else %}
        creation_time >= getdate() - interval '12 months'
      {% endif %}
      

), v2_pools AS ( -- uni v2 and sushiswap
    SELECT 
      p.block_timestamp  AS creation_time,
      p.tx_id AS creation_tx, 
      p.contract_addr AS factory_address, 
      -- assign a liquidity pool name based on the two tokens in the pool in the format 'Token1-Token2 LP', i.e. 'WETH-DAI LP'
      -- goes through a couple fallbacks, 
      ---- try ethereum_contracts
      ---- if the above is null, try cmc_assets
      ---- if the above is null, try to at least get a name instead of a symbol from ethereum_address_labels
      ---- if all else fails then just use the token contract address to yield an informative name
      COALESCE(a.meta:symbol,aa.symbol,aaa.address_name,p.event_inputs:token0) ||'-'||COALESCE(b.meta:symbol,bb.symbol,bbb.address_name,p.event_inputs:token1)||' LP' AS pool_name,
      REGEXP_REPLACE(p.event_inputs:pair,'\"','')   as pool_address, 
      REGEXP_REPLACE(p.event_inputs:token0,'\"','') as token0,
      REGEXP_REPLACE(p.event_inputs:token1,'\"','') as token1,
      'uniswap-v2' AS platform
    FROM {{ source('ethereum', 'ethereum_events_emitted') }} p -- {{ref('ethereum__events_emitted')}} p

    LEFT JOIN {{source('ethereum','ethereum_contracts')}} a ON REGEXP_REPLACE(p.event_inputs:token0,'\"','') = a.address

    LEFT JOIN {{source('shared', 'cmc_assets')}} aa
      ON REGEXP_REPLACE(p.event_inputs:token0,'\"','')        = aa.token_address

    LEFT JOIN {{source('ethereum', 'ethereum_address_labels')}} aaa 
      ON REGEXP_REPLACE(p.event_inputs:token0,'\"','') = aaa.address

    LEFT JOIN {{source('ethereum', 'ethereum_contracts')}}  b 
      ON REGEXP_REPLACE(p.event_inputs:token1,'\"','') = b.address

    LEFT JOIN {{source('shared', 'cmc_assets')}} bb 
      ON REGEXP_REPLACE(p.event_inputs:token1,'\"','')        = bb.token_address

    LEFT JOIN {{source('ethereum', 'ethereum_address_labels')}} bbb 
      ON REGEXP_REPLACE(p.event_inputs:token1,'\"','') = bbb.address

    WHERE p.event_name    = 'PairCreated'
    {% if is_incremental() %}
      AND creation_time >= getdate() - interval '2 days'
    {% else %}
      AND creation_time >= getdate() - interval '12 months'
    {% endif %}
)

-- get pool info 
SELECT * FROM
v2_pools
UNION
SELECT * FROM
v3_pools