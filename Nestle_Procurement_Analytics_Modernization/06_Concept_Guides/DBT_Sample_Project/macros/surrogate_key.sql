{#
    Small illustrative macro: hashes one or more columns into a single surrogate key.
    Usage:  {{ surrogate_key(['supplier_id']) }} as supplier_key
    (dbt_utils.generate_surrogate_key does the same thing, more robustly — this exists
    purely to show what that kind of macro looks like under the hood.)
#}
{% macro surrogate_key(columns) %}
    md5(
        {% for col in columns %}
            coalesce(cast({{ col }} as varchar), '')
            {% if not loop.last %} || '~' || {% endif %}
        {% endfor %}
    )
{% endmacro %}
