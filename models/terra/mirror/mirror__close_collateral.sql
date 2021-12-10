{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'mirror', 'mirror_close_collateral', 'address_labels']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price
  FROM
    {{ ref('terra__oracle_prices') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
GROUP BY
  1,
  2,
  3
),
prices_backup AS (
  SELECT
    DATE_TRUNC(
      'day',
      block_timestamp
    ) AS DAY,
    currency,
    symbol,
    AVG(price_usd) AS price
  FROM
    {{ ref('terra__oracle_prices') }}
  WHERE
    1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
GROUP BY
  1,
  2,
  3
),
msgs AS (
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value :execute_msg :send :msg :burn :position_idx AS collateral_id,
    msg_value :sender :: STRING AS sender,
    msg_value :contract :: STRING AS contract_address,
    l.address_name AS contract_label
  FROM
    {{ ref('silver_terra__msgs') }}
    m
    LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
    ON msg_value :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  WHERE
    msg_value :execute_msg :send :msg :burn IS NOT NULL
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
),
burns AS (
  SELECT
    tx_id,
    event_attributes :burn_amount [0] :amount / pow(
      10,
      6
    ) AS burn_amount,
    burn_amount * i.price AS burn_amount_usd,
    event_attributes :burn_amount [0] :denom :: STRING AS burn_currency,
    event_attributes :protocol_fee [0] :amount / pow(
      10,
      6
    ) AS protocol_fee_amount,
    protocol_fee_amount * f.price AS protocol_fee_amount_usd,
    event_attributes :protocol_fee [0] :denom :: STRING AS protocol_fee_currency
  FROM
    {{ ref('silver_terra__msg_events') }}
    t
    LEFT OUTER JOIN prices i
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = i.hour
    AND t.event_attributes :burn_amount [0] :denom :: STRING = i.currency
    LEFT OUTER JOIN prices f
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = f.hour
    AND t.event_attributes :protocol_fee [0] :denom :: STRING = f.currency
    LEFT OUTER JOIN prices_backup i_b
    ON DATE_TRUNC(
      'day',
      t.block_timestamp
    ) = i_b.day
    AND t.event_attributes :burn_amount [0] :denom :: STRING = i_b.currency
    LEFT OUTER JOIN prices_backup f_b
    ON DATE_TRUNC(
      'day',
      t.block_timestamp
    ) = f_b.day
    AND t.event_attributes :protocol_fee [0] :denom :: STRING = f_b.currency
  WHERE
    event_attributes :burn_amount IS NOT NULL
    AND event_attributes :protocol_fee IS NOT NULL
    AND tx_id IN(
      SELECT
        DISTINCT tx_id
      FROM
        msgs
    )
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
),
withdraw AS (
  SELECT
    tx_id,
    event_attributes :tax_amount [0] :amount / pow(
      10,
      6
    ) AS tax_amount,
    tax_amount * o.price AS tax_amount_usd,
    event_attributes :tax_amount [0] :denom :: STRING AS tax_currency,
    event_attributes :protocol_fee [0] :amount / pow(
      10,
      6
    ) AS protocol_fee_amount,
    protocol_fee_amount * f.price AS protocol_fee_amount_usd,
    event_attributes :protocol_fee [0] :denom :: STRING AS protocol_fee_currency,
    event_attributes :withdraw_amount [0] :amount / pow(
      10,
      6
    ) AS withdraw_amount,
    withdraw_amount * i.price AS withdraw_amount_usd,
    event_attributes :withdraw_amount [0] :denom :: STRING AS withdraw_currency
  FROM
    {{ ref('silver_terra__msg_events') }}
    t
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = o.hour
    AND t.event_attributes :tax_amount [0] :denom :: STRING = o.currency
    LEFT OUTER JOIN prices i
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = i.hour
    AND t.event_attributes :withdraw_amount [0] :denom :: STRING = i.currency
    LEFT OUTER JOIN prices f
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = f.hour
    AND t.event_attributes :protocol_fee [0] :denom :: STRING = f.currency
    LEFT OUTER JOIN prices_backup o_b
    ON DATE_TRUNC(
      'day',
      t.block_timestamp
    ) = o_b.day
    AND t.event_attributes :tax_amount [0] :denom :: STRING = o_b.currency
    LEFT OUTER JOIN prices_backup i_b
    ON DATE_TRUNC(
      'day',
      t.block_timestamp
    ) = i_b.day
    AND t.event_attributes :withdraw_amount [0] :denom :: STRING = i_b.currency
    LEFT OUTER JOIN prices_backup f_b
    ON DATE_TRUNC(
      'day',
      t.block_timestamp
    ) = f_b.day
    AND t.event_attributes :protocol_fee [0] :denom :: STRING = f_b.currency
  WHERE
    event_attributes :withdraw_amount IS NOT NULL
    AND event_attributes :"2_action" != 'release_shorting_funds'
    AND tx_id IN(
      SELECT
        DISTINCT tx_id
      FROM
        msgs
    )
    AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ ref('silver_terra__msgs') }}
)
{% endif %}
)
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  collateral_id,
  sender,
  tax_amount,
  tax_amount_usd,
  tax_currency,
  COALESCE(
    b.protocol_fee_amount,
    w.protocol_fee_amount
  ) AS protocol_fee_amount,
  COALESCE(
    b.protocol_fee_amount_usd,
    w.protocol_fee_amount_usd
  ) AS protocol_fee_amount_usd,
  COALESCE(
    b.protocol_fee_currency,
    w.protocol_fee_currency
  ) AS protocol_fee_currency,
  burn_amount,
  burn_amount_usd,
  burn_currency,
  withdraw_amount,
  withdraw_amount_usd,
  withdraw_currency,
  contract_address,
  contract_label
FROM
  msgs m
  JOIN burns b
  ON m.tx_id = b.tx_id
  JOIN withdraw w
  ON m.tx_id = w.tx_id
