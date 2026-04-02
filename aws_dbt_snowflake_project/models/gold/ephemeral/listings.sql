{% set configs = [
    {
        "table": "AIRBNB.GOLD.OBT",
        "columns":"OBT.BOOKING_ID, OBT.LISTING_ID, OBT.HOST_ID, OBT.TOTAL_AMOUNT, OBT.ACCOMMODATES, OBT.BEDROOMS, OBT.BATHROOMS, OBT.PRICE_PER_NIGHT, OBT.RESPONSE_RATE",
        "alias":"OBT"
    },

    {
        "table": "AIRBNB.GOLD.DIM_LISTINGS",
        "columns":"",
        "alias":"DIM_LISTINGS",
        "join_condition":"OBT.LISTING_ID = DIM_LISTINGS.LISTING_ID"
    },

    {
        "table": "AIRBNB.GOLD.DIM_HOSTS",
        "columns":"",
        "alias":"DIM_HOSTS",
        "join_condition":"OBT.HOST_ID = DIM_HOSTS.HOST_ID"
    }
]%}



SELECT
         {{ configs[0]['columns'] }}
FROM
    {% for config in configs %}
        {% if loop.first %}
            {{ config['table'] }} AS {{ config['alias'] }}
        {% else %}
            LEFT JOIN {{ config['table'] }} AS {{ config['alias'] }} ON {{ config['join_condition'] }}
        {% endif %}
    {% endfor %}