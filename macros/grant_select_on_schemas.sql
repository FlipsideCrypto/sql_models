{% macro grant_select_on_schemas(
        schemas,
        role
    ) %}
    {% for schema in schemas %}
        {{ grant_select_on_schema(
            schema,
            role
        ) }}
    {% endfor %}
{% endmacro %}
