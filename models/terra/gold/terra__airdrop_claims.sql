{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'airdrops', 'claims']
) }}

SELECT
  l.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value :execute_msg :claim :stage :: NUMBER AS airdrop_id,
  msg_value :sender :: STRING AS claimer,
  msg_value :execute_msg :claim :amount / pow(
    10,
    6
  ) AS amount,
  msg_value :contract :: STRING AS contract_address,
  l.address AS contract_label
FROM
  {{ ref('silver_terra__msgs') }}
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS l
  ON msg_value :contract :: STRING = l.address
WHERE
  msg_value :execute_msg :claim :amount IS NOT NULL
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