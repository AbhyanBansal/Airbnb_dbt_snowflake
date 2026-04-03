WITH bookings AS (
    SELECT
        b.BOOKING_ID,
        b.LISTING_ID,
        l.HOST_ID,
        b.BOOKING_DATE,
        b.BOOKING_STATUS,
        b.TOTAL_AMOUNT,
        b.CREATED_AT
    FROM {{ ref('silver_bookings') }} AS b
    LEFT JOIN {{ ref('silver_listings') }} AS l
        ON b.LISTING_ID = l.LISTING_ID
)

SELECT
    b.BOOKING_ID,
    b.LISTING_ID,
    b.HOST_ID,
    dl.DBT_SCD_ID AS LISTING_SCD_ID,
    dh.DBT_SCD_ID AS HOST_SCD_ID,
    b.BOOKING_DATE,
    b.BOOKING_STATUS,
    b.TOTAL_AMOUNT,
    b.CREATED_AT
FROM bookings AS b
LEFT JOIN {{ ref('dim_listings') }} AS dl
    ON b.LISTING_ID = dl.LISTING_ID
    AND b.CREATED_AT >= dl.DBT_VALID_FROM
    AND b.CREATED_AT < COALESCE(dl.DBT_VALID_TO, TO_TIMESTAMP_NTZ('9999-12-31'))
LEFT JOIN {{ ref('dim_hosts') }} AS dh
    ON b.HOST_ID = dh.HOST_ID
    AND b.CREATED_AT >= dh.DBT_VALID_FROM
    AND b.CREATED_AT < COALESCE(dh.DBT_VALID_TO, TO_TIMESTAMP_NTZ('9999-12-31'))
