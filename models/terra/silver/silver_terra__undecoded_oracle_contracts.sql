{{ config(
  materialized = 'incremental',
  unique_key = "token_contract",
  incremental_strategy = 'merge',
  tags = ['snowflake', 'terra', 'undecoded', 'terra_contracts']
) }}

SELECT
  DISTINCT msg_value :execute_msg :feed_price :prices [0] [0] :: STRING AS token_contract,
  'UNDECODED CW20' AS description,
  SYSDATE() AS _inserted_timestamp
FROM
  {{ ref("silver_terra__msgs") }}
WHERE
  msg_value :execute_msg :feed_price IS NOT NULL
  AND token_contract <> 'terra15gwkyepfc6xgca5t5zefzwy42uts8l2m4g40k6' --MIR contract is decoded
  AND tx_status = 'SUCCEEDED'
  AND token_contract NOT IN (
    SELECT
      address
    FROM
      {{ ref('silver_terra__contract_info') }}
  )

{% if is_incremental() %}
AND _inserted_timestamp >= COALESCE(
  (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  ),
  '1900-01-01'
)
{% endif %}
