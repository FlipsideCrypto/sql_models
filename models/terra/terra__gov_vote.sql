{{ config(
  materialized = 'incremental',
  sort = 'block_timestamp',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'terra', 'gov']
) }}

WITH balances AS (

  SELECT
    DATE,
    address,
    balance
  FROM
    {{ source(
      'terra',
      'udm_daily_balances_terra'
    ) }}
  WHERE
    balance_type = 'staked'
    AND address IN(
      SELECT
        DISTINCT msg_value :voter :: STRING
      FROM
        {{ ref('silver_terra__msgs') }}
      WHERE
        msg_module = 'gov'
        AND msg_type = 'gov/MsgVote'
        AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days' -- {% else %}
--   AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
)

{% if is_incremental() %}
AND DATE >= getdate() - INTERVAL '1 days' -- {% else %}
--  AND date >= getdate() - interval '9 months'
{% endif %}
)
SELECT
  A.blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  msg_type,
  REGEXP_REPLACE(
    msg_value :voter,
    '\"',
    ''
  ) AS voter,
  voter_labels.l1_label AS voter_label_type,
  voter_labels.l2_label AS voter_label_subtype,
  voter_labels.project_name AS voter_address_label,
  voter_labels.address AS voter_address_name,
  REGEXP_REPLACE(
    msg_value :proposal_id,
    '\"',
    ''
  ) AS proposal_id,
  REGEXP_REPLACE(
    msg_value :option,
    '\"',
    ''
  ) AS "OPTION",
  b.balance AS voting_power
FROM
  {{ ref('silver_terra__msgs') }} A
  LEFT OUTER JOIN {{ ref('silver_crosschain__address_labels') }} AS voter_labels
  ON msg_value :voter = voter_labels.address
  LEFT OUTER JOIN balances b
  ON DATE(
    A.block_timestamp
  ) = DATE(
    b.date
  )
  AND msg_value :voter :: STRING = b.address
WHERE
  msg_module = 'gov'
  AND msg_type = 'gov/MsgVote'
  AND tx_status = 'SUCCEEDED'

{% if is_incremental() %}
AND block_timestamp >= getdate() - INTERVAL '1 days' -- {% else %}
--  AND block_timestamp >= getdate() - interval '9 months'
{% endif %}
