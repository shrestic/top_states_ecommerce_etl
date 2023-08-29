COPY (
    select invoice_number,
           stock_code,
           detail,
           quantity,
           invoice_date,
           unit_price,
           customer_id,
           country
       from retail.user_purchase
     where quantity > 2
       and cast(invoice_date as date)='{{ ds }}')
TO '{{ params.user_purchase }}' WITH (FORMAT CSV, HEADER);

/*
The params.user_purchase variable is a placeholder for a parameter that is passed to the task. The parameter will be set to the path of the temporary file.

The ds variable is a special variable in Airflow that represents the current date. In this case, the COPY statement will only copy data from the user_purchase table for the current day.
*/