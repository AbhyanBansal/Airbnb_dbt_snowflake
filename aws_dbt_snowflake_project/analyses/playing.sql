{# {% set nights_booked=3 %}

SELECT * FROM {{ ref('bronze_bookings') }}
WHERE NIGHTS_BOOKED > {{ nights_booked }} #}


{% set flag = 1 %}

SELECT * FROM {{ ref('bronze_bookings') }}
{% if flag == 1 %}
WHERE NIGHTS_BOOKED > 1
{% else %}
WHERE NIGHTS_BOOKED = 1
{% endif %}