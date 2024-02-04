CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG DATABASE 'ecommerce_spectrum_db' iam_role 'arn:aws:iam::362262895301:role/Custom-RedShift-Role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
DROP TABLE IF EXISTS spectrum.user_behavior;
CREATE EXTERNAL TABLE spectrum.user_behavior (
    age INT,
    order_count INT
) STORED AS PARQUET LOCATION 's3://BUCKET_NAME/clean_data/' TABLE PROPERTIES ('skip.header.line.count' = '1');