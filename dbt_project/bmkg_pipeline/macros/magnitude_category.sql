{# Reusable macro: kategorisasi magnitude #}
{% macro magnitude_category(column_name) %}
    case
        when {{ column_name }} < 3.0 then 'micro'
        when {{ column_name }} < 4.0 then 'minor'
        when {{ column_name }} < 5.0 then 'light'
        when {{ column_name }} < 6.0 then 'moderate'
        when {{ column_name }} < 7.0 then 'strong'
        when {{ column_name }} < 8.0 then 'major'
        else                              'great'
    end
{% endmacro %}
