{{ config(
  materialized = 'incremental',
  unique_key = 'ADDRESS',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'account','silver_algorand']
) }}

SELECT
  algorand_decode_hex_addr(
    addr :: text
  ) AS address,
  deleted AS account_closed,
  rewardsbase / pow(
    10,
    6
  ) AS rewardsbase,
  rewards_total / pow(
    10,
    6
  ) AS rewards_total,
  microalgos / pow(
    10,
    6
  ) AS balance,
  closed_at AS closed_at,
  created_at AS created_at,
  keytype AS wallet_type,
  account_data AS account_data,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ source(
    'algorand',
    'ACCOUNT'
  ) }}
WHERE
  1 = 1

{% if is_incremental() %}
AND DATEADD(
  ms,
  __HEVO__LOADED_AT,
  '1970-01-01'
) >= (
  SELECT
    MAX(
      _INSERTED_TIMESTAMP
    )
  FROM
    {{ this }}
)
{% endif %}
