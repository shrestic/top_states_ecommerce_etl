COPY (
    SELECT id,
        first_name,
        last_name,
        email,
        age,
        gender,
        state,
        street_address,
        postal_code,
        city,
        country,
        latitude,
        longitude,
        traffic_source,
        to_date(cast(created_at as TEXT),'YYYY-MM-DD') as created_at
    FROM users
) TO STDOUT WITH (FORMAT CSV, HEADER);