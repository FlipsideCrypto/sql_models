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
  microalgos / pow(
    10,
    6
  ) AS balance,
  closed_at AS closed_at,
  created_at AS created_at,
  keytype AS wallet_type,
  account_data AS account_data,
  _FIVETRAN_SYNCED
FROM
  {{ source(
    'algorand',
    'ACCOUNT'
  ) }}
WHERE
  1 = 1

{% if is_incremental() %}
AND _FIVETRAN_SYNCED >= (
  SELECT
    MAX(
      _FIVETRAN_SYNCED
    )
  FROM
    {{ this }}
)
{% endif %}
