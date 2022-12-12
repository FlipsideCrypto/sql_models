{% macro set_query_tag() -%}
 {% set new_json = {"repo":project_name, "object":this.table, "profile":target.profile_name, "env":target.name, "existing_tag":get_current_query_tag()  } %}
{% set new_query_tag = tojson(new_json) | as_text %}
  {% if new_query_tag %}
    {% set original_query_tag = get_current_query_tag() %}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag)}}
  {% endif %}
  {{ return(none)}}
{% endmacro %}