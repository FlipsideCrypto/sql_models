{% macro uni_math_tick_to_price_1_0(decimals0, decimals1, tick) -%}
CASE 
  WHEN {{decimals0}} IS NULL OR {{decimals1}} IS NULL OR {{tick}} IS NULL THEN NULL
  -- We want a positive exponent
  ELSE pow(1.0001, {{tick}}) / pow(10, {{decimals1}} - {{decimals0}})
END
{%- endmacro %}

{% macro uni_math_tick_to_price_0_1(decimals0, decimals1, tick) -%}
CASE 
  WHEN {{decimals0}} IS NULL OR {{decimals1}} IS NULL OR {{tick}} IS NULL THEN NULL
  -- We want a negative exponent
  ELSE pow(pow(1.0001, {{tick}}) / pow(10, {{decimals1}} - {{decimals0}}),-1)
END
{%- endmacro %}