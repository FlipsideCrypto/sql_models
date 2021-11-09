{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "CONCAT_WS('-', chain_id, round_, tx)",
  cluster_by = ['system_created_at::DATE']
) }}

SELECT
  (
    record_metadata :CreateTime :: INT / 1000
  ) :: TIMESTAMP AS system_created_at,
  record_content :model :blockchain :: STRING AS chain_id,
  r.value :assetConfig AS assetConfig,
  r.value :assetFreeze AS assetFreeze,
  r.value :assetTransfer AS assetTransfer,
  r.value :fee AS fee,
  r.value :firstRound AS firstRound,
  r.value :from AS from_,
  r.value :fromRewards AS fromRewards,
  r.value :genesishashb64 AS genesishashb64,
  r.value :keyReg AS keyReg,
  r.value :lastRound AS lastRound,
  r.value :payment AS payment,
  r.value :round AS round_,
  r.value :tx AS tx,
  r.value :txResults AS txResults,
  r.value :type AS type_
FROM
  {{ source(
    'bronze',
    'prod_algorand_sink_991320494'
  ) }},
  LATERAL FLATTEN(
    input => record_content :results
  ) r
WHERE
  record_content :model.name = 'algorand_tx_model'

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

qualify(ROW_NUMBER() over(PARTITION BY chain_id, round_, tx
ORDER BY
  system_created_at DESC)) = 1
