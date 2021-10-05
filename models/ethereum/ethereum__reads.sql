{{ 
  config(
    materialized='incremental',
    unique_key='block_id || contract_address || function_name || inputs', 
    incremental_strategy='delete+insert',
    cluster_by=['block_timestamp', 'contract_address'],
    tags=['snowflake', 'ethereum', 'reads']
  )
}}

SELECT 
  BLOCK_ID AS block_id,
  BLOCK_TIMESTAMP AS block_timestamp,
  CONTRACT_ADDRESS AS contract_address,
  COALESCE(contract_labels.address_name,CONTRACT_NAME) AS contract_name,
  FUNCTION_NAME AS function_name,
  INPUTS AS inputs,
  PROJECT_ID AS project_id,
  b.PROJECT_NAME AS project_name,
  VALUE_NUMERIC AS value_numeric,
  VALUE_STR AS value_string,
  null::object as value_object
FROM {{ source('ethereum', 'ethereum_reads') }} b

LEFT OUTER JOIN {{ source('ethereum', 'ethereum_address_labels') }} as contract_labels
 ON b.CONTRACT_ADDRESS = contract_labels.address

WHERE
{% if is_incremental() %}
  b.block_timestamp >= getdate() - interval '40 hours'
{% else %}
  b.block_timestamp >= getdate() - interval '9 months'
{% endif %}

UNION

select block_id,
block_timestamp,
contract_address,
coalesce(l.address_name, contract_name) as contract_name,
function_name,
inputs,
project_id,
r.project_name,
value_numeric,
value_string,
value_object as value_object
from {{ ref('silver_ethereum__reads')}} r
left join {{ source('ethereum', 'ethereum_address_labels') }} as l
on r.contract_address = l.address
WHERE 
{% if is_incremental() %}
  r.block_timestamp >= getdate() - interval '40 hours'
{% else %}
  r.block_timestamp >= getdate() - interval '9 months'
{% endif %}