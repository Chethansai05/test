
-- Step 1: Create or replace a stage and load data into it
CREATE OR REPLACE STAGE sales_stage
URL='s3://my-sales-bucket/sales-data'
CREDENTIALS = (AWS_KEY_ID='your_access_key_id' AWS_SECRET_KEY='your_secret_key');

COPY INTO sales_stage
FROM 'sales_data.csv'
CREDENTIALS=(AWS_KEY_ID='your_access_key_id' AWS_SECRET_KEY='your_secret_key')
FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1);

-- Step 2: Transform data
-- Calculate total revenue for each sale
CREATE OR REPLACE TABLE transformed_sales AS
SELECT 
    product_id, 
    quantity_sold, 
    unit_price, 
    quantity_sold * unit_price AS total_revenue
FROM sales_stage;

-- Step 3: Load data into target table
-- Let's assume we have a target table called 'sales_summary'
INSERT INTO sales_summary (product_id, quantity_sold, unit_price, total_revenue)
SELECT product_id, quantity_sold, unit_price, total_revenue
FROM transformed_sales;
