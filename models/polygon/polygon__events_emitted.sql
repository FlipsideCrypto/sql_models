{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, event_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_id','block_timestamp'],
  tags = ['snowflake', 'polygon', 'polygon_events_emitted_gold', 'address_labels']
) }}

WITH poly_labels AS (

  SELECT
    l1_label,
    l2_label,
    project_name,
    address_name,
    address
  FROM
    {{ ref('silver_crosschain__address_labels') }}
  WHERE
    blockchain = 'polygon'
)
SELECT
  block_id AS block_id,
  block_timestamp AS block_timestamp,
  tx_id AS tx_id,
  event_index AS event_index,
  event_inputs AS event_inputs,
  event_name AS event_name,
  event_removed AS event_removed,
  tx_from AS tx_from_address,
  from_labels.l1_label AS tx_from_label_type,
  from_labels.l2_label AS tx_from_label_subtype,
  from_labels.project_name AS tx_from_label,
  from_labels.address_name AS tx_from_address_name,
  tx_to AS tx_to_address,
  to_labels.l1_label AS tx_to_label_type,
  to_labels.l2_label AS tx_to_label_subtype,
  to_labels.project_name AS tx_to_label,
  to_labels.address_name AS tx_to_address_name,
  contract_address AS contract_address,
  COALESCE(
    contract_labels.address,
    contract_name
  ) AS contract_name,
  tx_succeeded AS tx_succeeded
FROM
  {{ ref('silver_polygon__events_emitted') }}
  b
  LEFT OUTER JOIN poly_labels AS from_labels
  ON b.tx_from = from_labels.address
  LEFT OUTER JOIN poly_labels AS to_labels
  ON b.tx_to = to_labels.address
  LEFT OUTER JOIN poly_labels AS contract_labels
  ON b.contract_address = contract_labels.address
WHERE
  1 = 1

{% if is_incremental() %}
AND b.block_timestamp :: DATE >= (
  SELECT
    MAX(
      block_timestamp :: DATE
    )
  FROM
    {{ this }}
)
{% endif %}
