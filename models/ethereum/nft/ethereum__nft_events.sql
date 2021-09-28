{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft']
) }}

with nft as (
  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__art_blocks_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__art_blocks_sales') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__ck_bids') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__ck_lists') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__ck_unlists') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__ck_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__hashmasks_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__hashmasks_sales') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__known_origin_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__makersplace_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION
  
  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__makersplace_sales') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__nifty_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__opensea_sales') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__opensea_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__polkamon_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__rarible_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__rarible_sales') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__sandbox_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__superrare_buys') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__superrare_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__superrare_accept_bids') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT
    * 
  FROM {{ ref('ethereum_dbt__superrare_auction_wins') }}
  WHERE 1=1
  {% if is_incremental() %}
      and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

  UNION

  SELECT
    * 
  FROM {{ ref('ethereum_dbt__zora_mints') }}
  WHERE 1=1
  {% if is_incremental() %}
     and block_timestamp >= getdate() - interval '5 days'
  {% endif %}

),
price AS (
  SELECT
    * 
  FROM (
    SELECT 
      symbol,
      hour,
      price,
      row_number() OVER(PARTITION BY symbol, hour ORDER BY hour DESC) as rn
    FROM {{ ref('ethereum__token_prices_hourly') }}
    WHERE 1=1
    {% if is_incremental() %}
     and hour >= getdate() - interval '5 days'
    {% endif %}
  )
  WHERE rn = 1 
)
WHERE
  rn = 1
)
SELECT
  nft.event_platform,
  nft.tx_id,
  nft.block_timestamp,
  nft.event_type,
  nft.contract_address,
  REGEXP_REPLACE(
    contract_labels.project_name,
    ' ',
    '_'
  ) AS project_name,
  nft.token_id,
  REGEXP_REPLACE(
    nft.event_from,
    '\"',
    ''
  ) AS event_from,
  REGEXP_REPLACE(
    nft.event_to,
    '\"',
    ''
  ) AS event_to,
  nft.price,
  nft.price * p.price AS price_usd,
  nft.platform_fee,
  nft.creator_fee,
  nft.tx_currency
FROM
  nft
  LEFT OUTER JOIN price p
  ON tx_currency = symbol
  AND DATE_TRUNC(
    'hour',
    block_timestamp
  ) = p.hour
  LEFT OUTER JOIN {{ source(
    'ethereum',
    'ethereum_address_labels'
  ) }} AS contract_labels
  ON nft.contract_address = contract_labels.address
