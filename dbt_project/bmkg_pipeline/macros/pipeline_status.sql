{# Reusable macro: hitung pipeline_status dari threshold vars #}
{% macro pipeline_status(freshness_col, major_col, invalid_col) %}
    case
        when {{ freshness_col }} > {{ var('freshness_fail_hours', 12) }}  then 'FAILED'
        when {{ freshness_col }} > {{ var('freshness_warn_hours', 2) }}
          or {{ major_col }} > 0
          or {{ invalid_col }} > 0                                         then 'WARNING'
        else 'OK'
    end
{% endmacro %}
