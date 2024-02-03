COPY (
    SELECT order_id,
        user_id,
        status,
        gender,
        created_at,
        returned_at,
        shipped_at,
        delivered_at,
        num_of_item
    FROM public.orders
    WHERE to_date(cast(created_at as TEXT),'YYYY-MM-DD') = {{ ds }}
    ORDER BY created_at;
) TO STDOUT WITH (FORMAT CSV, HEADER);