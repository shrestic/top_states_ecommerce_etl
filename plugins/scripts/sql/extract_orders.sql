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
    WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', TIMESTAMP {start_date}) 
    ORDER BY created_at
) TO STDOUT WITH (FORMAT CSV, HEADER);