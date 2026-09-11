{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as row_count
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{% test between_values(model, column_name, minimum, maximum) %}
select * from {{ model }}
where {{ column_name }} < {{ minimum }} or {{ column_name }} > {{ maximum }}
{% endtest %}
