{{ config(
  materialized = 'incremental',
  unique_key = "token_contract",
  incremental_strategy = 'merge',
  tags = ['snowflake', 'terra', 'undecoded', 'terra_contracts']
) }}

SELECT
  event_attributes :contract_address :: STRING AS token_contract,
  'WORMHOLE' AS description,
  SYSDATE() AS _inserted_timestamp
FROM
  {{ ref("silver_terra__msg_events") }}
WHERE
  event_attributes :creator = 'terra10nmmwe8r3g99a9newtqa7a75xfgs2e8z87r2sf' --wormhole contracts
  AND block_timestamp > '2021-06-01'
  AND token_contract NOT IN (
    SELECT
      decoded_contract
    FROM
      {{ ref('silver_terra__contract_info') }}
  )
