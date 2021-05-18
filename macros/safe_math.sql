{% macro multiply(col_a, col_b) -%}

CASE WHEN {{col_a}} IS NOT NULL AND {{col_b}} IS NOT NULL
    THEN {{col_a}} * {{col_b}}
    ELSE NULL
END

{%- endmacro %}
