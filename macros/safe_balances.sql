{% macro safe_ethereum_balances(src_balances_table, incremental_days = '30 days', full_refresh_days = '9 months') -%}

  SELECT *
  FROM
  (
    SELECT 
      *,
      row_number() OVER (PARTITION BY balance_date, address, contract_address, symbol ORDER BY balance_date DESC) AS rn
    FROM {{ src_balances_table }}
    WHERE
    {% if is_incremental() %}
    balance_date >= getdate() - interval '{{incremental_days}}'
    {% else %}
    balance_date >= getdate() - interval '{{full_refresh_days}}'
    {% endif %}
  ) sq
  WHERE
  sq.rn = 1

{%- endmacro %}