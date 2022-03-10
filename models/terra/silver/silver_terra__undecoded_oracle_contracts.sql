{{ config(
  materialized = 'incremental',
  unique_key = "token_contract",
  incremental_strategy = 'merge',
  tags = ['snowflake', 'terra', 'undecoded', 'terra_contracts']
) }}

SELECT DISTINCT
  msg_value :execute_msg :feed_price :prices [0] [0] :: STRING AS token_contract,
  'UNDECODED CW20' AS description,
  SYSDATE() AS _inserted_timestamp
FROM
  {{ ref("silver_terra__msgs") }}
WHERE 
  msg_value :execute_msg :feed_price IS NOT NULL
  AND block_timestamp::date >= current_date - 30
  AND token_contract <> 'terra15gwkyepfc6xgca5t5zefzwy42uts8l2m4g40k6' --MIR contract is decoded
  AND token_contract NOT IN (SELECT decoded_contract FROM {{ ref('terra_dbt__decoded_contracts') }})