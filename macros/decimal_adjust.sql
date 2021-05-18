{% macro decimal_adjust(col_to_adjust, decimal_col) -%}

CASE WHEN {{col_to_adjust}} IS NOT NULL AND {{decimal_col}} IS NOT NULL
    THEN {{col_to_adjust}} / pow(10, {{decimal_col}}) 
    ELSE NULL
END

{%- endmacro %}


{% macro decimal_adjust_with_inputs(col_to_adjust, num, denom) -%}

CASE WHEN {{col_to_adjust}} IS NOT NULL AND {{denom}} IS NOT NULL
    THEN {{num}} / pow(10, {{denom}}) 
    ELSE NULL
END

{%- endmacro %}
