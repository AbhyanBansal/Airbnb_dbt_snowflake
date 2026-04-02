{# JINJA IF INVALID FOR COLUMN COMPARISONS #}
{# {% macro tag(col) %}
    {% if col < 100 %}
        'LOW'
    {% elif col < 200 %}
        'MEDIUM'
    {% else %}
        'HIGH'
    {% endif %}
{% endmacro %} #}

{% macro tag(col) %}
    CASE 
        WHEN {{ col }} < 100 THEN 'LOW'
        WHEN {{ col }} < 200 THEN 'MEDIUM'
        ELSE 'HIGH'
    END
{% endmacro %}