{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "CONCAT_WS('-', chain_id, round_)",
  cluster_by = ['system_created_at::DATE']
) }}

SELECT
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  record_content :model :blockchain :: STRING AS chain_id,
  r.value :currentProtocol AS "currentProtocol",
  r.value :frac AS "frac",
  r.value :hash AS "hash",
  r.value :nextProtocol AS "nextProtocol",
  r.value :nextProtocolApprovals AS "nextProtocolApprovals",
  r.value :nextProtocolSwitchOn AS "nextProtocolSwitchOn",
  r.value :nextProtocolVoteBefore AS "nextProtocolVoteBefore",
  r.value :period AS "period",
  r.value :previousBlockHash AS "previousBlockHash",
  r.value :proposer AS "proposer",
  r.value :rate AS "rate",
  r.value :reward AS "reward",
  r.value :round AS round_,
  r.value :seed AS "seed",
  r.value :timestamp :: TIMESTAMP AS timestamp_,
  r.value :transactionCount AS "transactionCount",
  r.value :txnRoot AS "txnRoot",
  r.value :upgradeApprove AS "upgradeApprove",
  r.value :upgradePropose AS "upgradePropose"
FROM
  {{ source(
    'bronze',
    'prod_algorand_sink_991320494'
  ) }},
  LATERAL FLATTEN(
    input => record_content :results
  ) r
WHERE
  record_content :model.name = 'algorand_block_model'

{% if is_incremental() %}
AND (
  record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY chain_id, round_
ORDER BY
  system_created_at DESC)) = 1
