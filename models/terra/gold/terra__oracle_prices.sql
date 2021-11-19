{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "CONCAT_WS('-', block_timestamp)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'terra', 'oracle', 'terra_oracle']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS block_timestamp,
    symbol AS currency,
    AVG(price) AS price
  FROM
    {{ source(
      'shared',
      'prices_v2'
    ) }}
  WHERE
    asset_id = '4172'
  GROUP BY
    1,
    2
),
other_prices AS (
  SELECT
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS block_timestamp,
    symbol AS currency,
    AVG(price) AS price
  FROM
    {{ source(
      'shared',
      'prices_v2'
    ) }}
  WHERE
    asset_id IN(
      '7857',
      '8857'
    )
  GROUP BY
    1,
    2
),
luna_rate AS (
  SELECT
    blockchain,
    chain_id,
    block_timestamp,
    block_id,
    REGEXP_REPLACE(
      event_attributes :denom :: STRING,
      '\"',
      ''
    ) AS currency,
    event_attributes :exchange_rate :: FLOAT AS exchange_rate
  FROM
    {{ ref('silver_terra__transitions') }}
  WHERE
    event = 'exchange_rate_update'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days'
{% else %}
  AND block_timestamp >= getdate() - INTERVAL '9 months'
{% endif %}
),
massets AS(
  SELECT
    m.blockchain,
    m.chain_id,
    m.block_timestamp,
    m.block_id,
    m.msg_value :execute_msg :feed_price :prices [0] [0] :: STRING AS currency,
    p.address AS symbol,
    m.msg_value :execute_msg :feed_price :prices [0] [1] :: FLOAT AS price
  FROM
    {{ ref('silver_terra__msgs') }}
    m
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }}
    p
    ON msg_value :execute_msg :feed_price :prices [0] [0] :: STRING = p.address AND p.blockchain = 'terra' and p.creator = 'flipside'
  WHERE
    msg_value :contract = 'terra1t6xe0txzywdg85n6k8c960cuwgh6l8esw6lau9' --Mirror Oracle Feeder
    AND msg_value :sender = 'terra128968w0r6cche4pmf4xn5358kx2gth6tr3n0qs' -- Make sure we are pulling right events

{% if is_incremental() %}
AND m.block_timestamp >= getdate() - INTERVAL '1 days'
{% endif %}
)
SELECT
  blockchain,
  l.block_timestamp,
  l.currency,
  CASE
    WHEN l.currency = 'usgd' THEN 'SGT'
    WHEN l.currency = 'uusd' THEN 'UST'
    WHEN l.currency = 'ukrw' THEN 'KRT'
    WHEN l.currency = 'unok' THEN 'NOT'
    WHEN l.currency = 'ucny' THEN 'CNT'
    WHEN l.currency = 'uinr' THEN 'INT'
    WHEN l.currency = 'ueur' THEN 'EUT'
    WHEN l.currency = 'udkk' THEN 'DKT'
    WHEN l.currency = 'uhkd' THEN 'HKT'
    WHEN l.currency = 'usek' THEN 'SET'
    WHEN l.currency = 'uthb' THEN 'THT'
    WHEN l.currency = 'umnt' THEN 'MNT'
    WHEN l.currency = 'ucad' THEN 'CAT'
    WHEN l.currency = 'ugbp' THEN 'GBT'
    WHEN l.currency = 'ujpy' THEN 'JPT'
    WHEN l.currency = 'usdr' THEN 'SDT'
    WHEN l.currency = 'uchf' THEN 'CHT'
    WHEN l.currency = 'uaud' THEN 'AUT'
    ELSE l.currency
  END AS symbol,
  exchange_rate AS luna_exchange_rate,
  CASE
  WHEN (price/exchange_rate) IS NULL 
  THEN (last_value(price ignore nulls) over (partition by symbol order by l.block_timestamp asc rows between unbounded preceding and current row))/exchange_rate
  else price/exchange_rate 
  END AS price_usd,
  'oracle' AS source
FROM
  luna_rate l
  LEFT OUTER JOIN prices p
  ON DATE_TRUNC(
    'hour',
    l.block_timestamp
  ) = p.block_timestamp
UNION
SELECT
  'terra' AS blockchain,
  block_timestamp,
  'uluna' AS currency,
  'LUNA' AS symbol,
  1 AS luna_exchange_rate,
  price AS price_usd,
  'coinmarketcap' AS source
FROM
  prices
UNION
SELECT
  'terra' AS blockchain,
  o.block_timestamp,
  CASE
    WHEN o.currency = 'MIR' THEN 'terra15gwkyepfc6xgca5t5zefzwy42uts8l2m4g40k6'
    WHEN o.currency = 'ANC' THEN 'terra14z56l0fp2lsf86zy3hty2z47ezkhnthtr9yq76'
    ELSE NULL
  END AS currency,
  o.currency AS symbol,
  x.price / o.price AS luna_exchange_rate,
  o.price AS price_usd,
  'coinmarketcap' AS source
FROM
  other_prices o
  LEFT OUTER JOIN prices x
  ON DATE_TRUNC(
    'hour',
    o.block_timestamp
  ) = x.block_timestamp
UNION
SELECT
  ma.blockchain,
  ma.block_timestamp,
  ma.currency AS currency,
  ma.symbol AS symbol,
  pp.price / ma.price AS luna_exchange_rate,
  ma.price AS price_usd,
  'oracle' AS source
FROM
  massets ma
  LEFT OUTER JOIN prices pp
  ON DATE_TRUNC(
    'hour',
    ma.block_timestamp
  ) = pp.block_timestamp
UNION
SELECT
  ee.blockchain,
  ee.block_timestamp,
  ee.event_attributes :asset :: STRING AS currency,
  l.address AS symbol,
  pp.price / ee.event_attributes :price :: FLOAT AS luna_exchange_rate,
  ee.event_attributes :price :: FLOAT AS price_usd,
  'oracle' AS source
FROM
  {{ ref('silver_terra__msg_events') }}
  ee
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON ee.event_attributes :asset :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  LEFT OUTER JOIN prices pp
  ON DATE_TRUNC(
    'hour',
    ee.block_timestamp
  ) = pp.block_timestamp
WHERE
  event_type = 'from_contract'
  AND tx_id IN(
    SELECT
      tx_id
    FROM
      {{ ref('silver_terra__msgs') }}
    WHERE
      msg_value :contract :: STRING = 'terra1cgg6yef7qcdm070qftghfulaxmllgmvk77nc7t'
      AND msg_value :execute_msg :feed_price IS NOT NULL
  )