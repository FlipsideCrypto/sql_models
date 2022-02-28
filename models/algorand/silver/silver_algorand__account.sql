{{ config(
  materialized = 'incremental',
  unique_key = 'ADDRESS',
  incremental_strategy = 'merge',
  tags = ['snowflake', 'algorand', 'account','silver_algorand']
) }}

SELECT
  algorand_decode_hex_addr(
    aa.addr :: text
  ) AS address,
  aa.deleted AS account_closed,
  aa.rewardsbase / pow(
    10,
    6
  ) AS rewardsbase,
  aa.rewards_total / pow(
    10,
    6
  ) AS rewards_total,
  aa.microalgos / pow(
    10,
    6
  ) AS balance,
  aa.closed_at AS closed_at,
  aa.created_at AS created_at,
  ab.block_timestamp AS created_at_timestamp,
  aa.keytype AS wallet_type,
  aa.account_data AS account_data,
  aa._FIVETRAN_SYNCED
FROM
  {{ source(
    'algorand',
    'ACCOUNT'
  ) }}
  aa
  LEFT JOIN {{ ref('silver_algorand__block') }}
  ab
  ON aa.created_at = ab.block_id
WHERE
  1 = 1

{% if is_incremental() %}
AND aa._FIVETRAN_SYNCED >= (
  SELECT
    MAX(
      _FIVETRAN_SYNCED
    )
  FROM
    {{ this }}
)
{% endif %}
