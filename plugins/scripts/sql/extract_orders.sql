COPY (
    SELECT id,
        order_id,
        user_id,
        product_id,
        inventory_item_id,
        status,
        to_date(cast(created_at as TEXT),'YYYY-MM-DD') as created_at,
        to_date(cast(shipped_at as TEXT),'YYYY-MM-DD') as shipped_at,
        to_date(cast(delivered_at as TEXT),'YYYY-MM-DD') as delivered_at,
        to_date(cast(returned_at as TEXT),'YYYY-MM-DD') as returned_at,
        sale_price
    FROM public.orders
    WHERE to_date(cast(created_at as TEXT),'YYYY-MM-DD') = {start_date}
    ORDER BY created_at
) TO STDOUT WITH (FORMAT CSV, HEADER);