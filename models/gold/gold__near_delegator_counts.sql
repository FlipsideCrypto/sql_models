{{ config(
  materialized='table',
  cluster_by=['date'],
  tags=['snowflake', 'gold', 'near', 'gold__near_delegator_counts'],
)}}
WITH near_labels AS (
    SELECT 
        l1_label,
        l2_label,
        project_name,
        address_name,
        address
    FROM {{ source('shared', 'udm_address_labels') }}
    WHERE blockchain = 'near'
)
SELECT
    xfer_date as date,
    validator as validator_address,
    l.l1_label as validator_label_type,
    l.l2_label as validator_label_subtype,
    l.project_name as validator_label,
    l.address_name as validator_name,
    n_delegators as delegator_count
FROM {{ source('near', 'near_delegator_counts') }} v
LEFT OUTER JOIN near_labels as l ON v.validator = l.address
