{{ config(
  materialized = 'incremental',
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'ethereum', 'nft', 'ethereum_nft_events', 'address_labels']
) }}

WITH nft AS (

  SELECT
    *
  FROM
    {{ ref('ethereum_dbt__art_blocks_mints') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__art_blocks_sales') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__ck_bids') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__ck_lists') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__ck_unlists') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__ck_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__hashmasks_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__hashmasks_sales') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__known_origin_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__makersplace_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__makersplace_sales') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__nifty_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__opensea_sales') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__opensea_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__polkamon_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__rarible_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__rarible_sales') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__sandbox_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__superrare_buys') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__superrare_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__superrare_accept_bids') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__superrare_auction_wins') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__zora_mints') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
UNION
SELECT
  *
FROM
  {{ ref('ethereum_dbt__looksrare_sales') }}

{% if is_incremental() %}
WHERE
  block_timestamp >= getdate() - INTERVAL '5 days'
{% endif %}
),
price AS (
  SELECT
    *
  FROM
    (
      SELECT
        symbol,
        HOUR,
        price,
        ROW_NUMBER() over(
          PARTITION BY symbol,
          HOUR
          ORDER BY
            HOUR DESC
        ) AS rn
      FROM
        {{ ref('ethereum__token_prices_hourly') }}
      WHERE
        1 = 1

{% if is_incremental() %}
AND HOUR >= getdate() - INTERVAL '5 days'
{% endif %}
)
WHERE
  rn = 1
),
decimals AS (
  SELECT
    LOWER(address) AS address,
    meta :decimals :: INTEGER AS decimals
  FROM
    {{ ref('ethereum__contracts') }}
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
  nft.event_from :: STRING AS event_from,
  nft.event_to :: STRING AS event_to,
  nft.price,
  CASE
    WHEN decimals IS NULL THEN NULL
    ELSE nft.price * p.price
  END AS price_usd,
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
  LEFT OUTER JOIN {{ ref(
    'silver_crosschain__address_labels'
  ) }} AS contract_labels
  ON nft.contract_address = contract_labels.address
  AND contract_labels.blockchain = 'ethereum'
  AND contract_labels.creator = 'flipside'
  LEFT JOIN decimals
  ON nft.contract_address = decimals.address
