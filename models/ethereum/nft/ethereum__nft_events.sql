{{ 
  config(
    materialized='incremental', 
    sort='block_timestamp',
    unique_key='tx_id', 
    incremental_strategy='delete+insert',
    tags=['snowflake', 'ethereum', 'nft']
  )
}}

with nft as (
  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__art_blocks_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__art_blocks_sales') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__ck_bids') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__ck_lists') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__ck_unlists') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '5 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__ck_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__hashmasks_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__hashmasks_sales') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__known_origin_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__makersplace_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION
  
  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__makersplace_sales') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '2 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__nifty_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__opensea_sales') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__opensea_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__polkamon_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '5 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__rarible_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__rarible_sales') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '2 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__sandbox_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__superrare_buys') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__superrare_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT 
    * 
  FROM {{ ref('ethereum_dbt__superrare_accept_bids') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT
    * 
  FROM {{ ref('ethereum_dbt__superrare_auction_wins') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

  UNION

  SELECT
    * 
  FROM {{ ref('ethereum_dbt__zora_mints') }}
  WHERE 
  {% if is_incremental() %}
      block_timestamp >= getdate() - interval '1 days'
  {% else %}
      block_timestamp >= getdate() - interval '9 months'
  {% endif %}

),

price as (
  SELECT
    * 
  FROM (
    SELECT 
      symbol,
      hour,
      price,
      row_number() OVER(PARTITION BY symbol, hour ORDER BY hour DESC) as rn
    FROM {{ ref('ethereum__token_prices_hourly') }}
    WHERE 
    {% if is_incremental() %}
      hour >= getdate() - interval '1 days'
    {% else %}
      hour >= getdate() - interval '9 months'
    {% endif %}
  )
  WHERE rn = 1 
)


SELECT 
  nft.event_platform,
  nft.tx_id,
  nft.block_timestamp,
  nft.event_type,
  nft.contract_address,
  REGEXP_REPLACE(contract_labels.project_name,' ','_') as project_name,
  nft.token_id,
  REGEXP_REPLACE(nft.event_from,'\"','') as event_from,
  REGEXP_REPLACE(nft.event_to,'\"','') as event_to,
  nft.price,
  nft.price * p.price as price_usd,
  nft.platform_fee,
  nft.creator_fee,
  nft.tx_currency
FROM nft

LEFT OUTER JOIN price p 
  ON tx_currency = symbol 
  AND date_trunc('hour', block_timestamp) = p.hour

LEFT OUTER JOIN {{ source('ethereum', 'ethereum_address_labels') }} as contract_labels
    ON nft.contract_address = contract_labels.address
