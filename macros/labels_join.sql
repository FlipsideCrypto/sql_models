{% macro labels_join(source_join_col, blockchain, alias) -%}
    {{ source('shared', 'udm_address_labels')}} as {{ alias }}
    ON {{ source_join_col }} = {{ alias }}.address AND {{ alias }}.blockchain = '{{ blockchain }}'
{%- endmacro %}