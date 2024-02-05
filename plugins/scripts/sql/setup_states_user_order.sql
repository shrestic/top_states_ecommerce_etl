CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG DATABASE 'states_user_order_db' iam_role 'arn:aws:iam::362262895301:role/Custom-RedShift-Role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
DROP TABLE IF EXISTS spectrum.states_user_order;
CREATE EXTERNAL TABLE spectrum.states_user_order (
    state VARCHAR,
    user_count INT,
    order_count INT,
    combined_count INT
) STORED AS PARQUET LOCATION 's3://BUCKET_NAME/transformed-data/' TABLE PROPERTIES ('skip.header.line.count' = '1');