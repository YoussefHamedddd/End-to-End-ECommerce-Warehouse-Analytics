-- =================================================================
-- CREATE TABLES FOR BRONZE LAYER (RAW DATA)
-- These tables store the raw data as-is from the source (OLTP)
-- No primary keys, no constraints, no aggregation.
-- =================================================================

-- DROP table if exists
IF OBJECT_ID('bronze.customers', 'U') IS NOT NULL
    DROP TABLE bronze.customers;

   CREATE TABLE bronze.customers (
    customer_id  NVARCHAR(255),
    customer_unique_id  NVARCHAR(255),
    customer_zip_code_prefix INT,
    customer_city NVARCHAR(MAX),
    customer_state NVARCHAR(255)
);

IF OBJECT_ID('bronze.geolocation', 'U') IS NOT NULL
    DROP TABLE bronze.geolocation;


CREATE TABLE bronze.geolocation (
    geolocation_zip_code_prefix INT , 
    geolocation_lat FLOAT, 
    geolocation_lng FLOAT,
    geolocation_city NVARCHAR(255),
    geolocation_state NVARCHAR(50) 
);


IF OBJECT_ID('bronze.order_items', 'U') IS NOT NULL
    DROP TABLE bronze.order_items;

CREATE TABLE bronze.order_items (
    order_id VARCHAR(255),
    order_item_id INT,
    product_id VARCHAR(255),
    seller_id VARCHAR(255),
    shipping_limit_date DATETIME2(3),
    price DECIMAL(18,2),
    freight_value DECIMAL(18,2)
);



IF OBJECT_ID('bronze.order_payments', 'U') IS NOT NULL
    DROP TABLE bronze.order_payments;

CREATE TABLE bronze.order_payments (
    order_id VARCHAR(200),
    payment_sequential INT,
    payment_type VARCHAR(200),
    payment_installments INT,
    payment_value DECIMAL(18,2)
);



IF OBJECT_ID('bronze.order_reviews', 'U') IS NOT NULL
    DROP TABLE bronze.order_reviews;

CREATE TABLE bronze.order_reviews (
    review_id VARCHAR(200),
    order_id VARCHAR(200),
    review_score INT,
    review_comment_title VARCHAR(MAX),
    review_comment_message VARCHAR(MAX),
    review_creation_date DATETIME2(3),
    review_answer_timestamp DATETIME2(3)
);



IF OBJECT_ID('bronze.orders', 'U') IS NOT NULL
    DROP TABLE bronze.orders;

CREATE TABLE bronze.orders (
    order_id VARCHAR(255),
    customer_id VARCHAR(255),
    order_status VARCHAR(150),
    order_purchase_timestamp DATETIME2(3),
    order_approved_at DATETIME2(3),
    order_delivered_carrier_date DATETIME2(3),
    order_delivered_customer_date DATETIME2(3),
    order_estimated_delivery_date DATETIME2(3)
);



IF OBJECT_ID('bronze.products', 'U') IS NOT NULL
    DROP TABLE bronze.products;

CREATE TABLE bronze.products (
    product_id VARCHAR(255),
    product_category_name VARCHAR(255),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);



IF OBJECT_ID('bronze.sellers', 'U') IS NOT NULL
    DROP TABLE bronze.sellers;

CREATE TABLE bronze.sellers (
    seller_id VARCHAR(255),
    seller_zip_code_prefix INT,
    seller_city VARCHAR(MAX),
    seller_state VARCHAR(50) 
);

IF OBJECT_ID('bronze.product_category_name_translation', 'U') IS NOT NULL
    DROP TABLE bronze.product_category_name_translation;

CREATE TABLE bronze.product_category_name_translation (
    product_category_name VARCHAR(255),
    product_category_name_english VARCHAR(255)
);


-- ddl for staging tables
-- ------------------------------------------------------------------------------------

 IF OBJECT_ID('bronze.customers_staging', 'U') IS NOT NULL
    DROP TABLE bronze.customers_staging;
CREATE TABLE bronze.customers_staging (
    customer_id NVARCHAR(MAX),
    customer_unique_id NVARCHAR(MAX),
    customer_zip_code_prefix NVARCHAR(MAX), 
    customer_city NVARCHAR(MAX),
    customer_state NVARCHAR(MAX)
);





 IF OBJECT_ID('bronze.geolocation_staging', 'U') IS NOT NULL
    DROP TABLE bronze.geolocation_staging;
 CREATE TABLE bronze.geolocation_staging (
    geolocation_zip_code_prefix NVARCHAR(MAX),
    geolocation_lat NVARCHAR(MAX),
    geolocation_lng NVARCHAR(MAX),
    geolocation_city NVARCHAR(MAX),
    geolocation_state NVARCHAR(MAX)
);


IF OBJECT_ID('bronze.order_items_staging', 'U') IS NOT NULL DROP TABLE bronze.order_items_staging;
CREATE TABLE bronze.order_items_staging (
    order_id NVARCHAR(MAX),
    order_item_id NVARCHAR(MAX),
    product_id NVARCHAR(MAX),
    seller_id NVARCHAR(MAX),
    shipping_limit_date NVARCHAR(MAX),
    price NVARCHAR(MAX),
    freight_value NVARCHAR(MAX)
);


		
		IF OBJECT_ID('bronze.order_payments_staging', 'U') IS NOT NULL DROP TABLE bronze.order_payments_staging;
		CREATE TABLE bronze.order_payments_staging (
			order_id NVARCHAR(MAX),
			payment_sequential NVARCHAR(MAX),
			payment_type NVARCHAR(MAX),
			payment_installments NVARCHAR(MAX),
			payment_value NVARCHAR(MAX)
		);



IF OBJECT_ID('bronze.order_reviews_staging', 'U') IS NOT NULL DROP TABLE bronze.order_reviews_staging;
CREATE TABLE bronze.order_reviews_staging (
    review_id NVARCHAR(MAX),
    order_id NVARCHAR(MAX),
    review_score NVARCHAR(MAX),
    review_comment_title NVARCHAR(MAX),
    review_comment_message NVARCHAR(MAX),
    review_creation_date NVARCHAR(MAX),
    review_answer_timestamp NVARCHAR(MAX)
);




		
IF OBJECT_ID('bronze.orders_staging', 'U') IS NOT NULL DROP TABLE bronze.orders_staging;
CREATE TABLE bronze.orders_staging (
    order_id NVARCHAR(MAX),
    customer_id NVARCHAR(MAX),
    order_status NVARCHAR(MAX),
    order_purchase_timestamp NVARCHAR(MAX),
    order_approved_at NVARCHAR(MAX),
    order_delivered_carrier_date NVARCHAR(MAX),
    order_delivered_customer_date NVARCHAR(MAX),
    order_estimated_delivery_date NVARCHAR(MAX)
);


IF OBJECT_ID('bronze.products_staging', 'U') IS NOT NULL DROP TABLE bronze.products_staging;
CREATE TABLE bronze.products_staging (
    product_id NVARCHAR(MAX),
    product_category_name NVARCHAR(MAX),
    product_name_length NVARCHAR(MAX),      
    product_description_length NVARCHAR(MAX), 
    product_photos_qty NVARCHAR(MAX),
    product_weight_g NVARCHAR(MAX),
    product_length_cm NVARCHAR(MAX),
    product_height_cm NVARCHAR(MAX),
    product_width_cm NVARCHAR(MAX)
);




IF OBJECT_ID('bronze.sellers_staging', 'U') IS NOT NULL DROP TABLE bronze.sellers_staging;
CREATE TABLE bronze.sellers_staging (
    seller_id NVARCHAR(MAX),
    seller_zip_code_prefix NVARCHAR(MAX),
    seller_city NVARCHAR(MAX),
    seller_state NVARCHAR(MAX)
);



IF OBJECT_ID('bronze.product_category_name_translation_staging', 'U') IS NOT NULL DROP TABLE bronze.product_category_name_translation_staging;
CREATE TABLE bronze.product_category_name_translation_staging (
    product_category_name NVARCHAR(MAX),
    product_category_name_english NVARCHAR(MAX)
);



-- =================================================================
-- DROP ALL STAGING TABLES
-- These tables are temporary and are dropped after data is moved 
-- to the permanent BRONZE or SILVER layers.
-- =================================================================

IF OBJECT_ID('bronze.customers_staging', 'U') IS NOT NULL
    DROP TABLE bronze.customers_staging;

IF OBJECT_ID('bronze.geolocation_staging', 'U') IS NOT NULL
    DROP TABLE bronze.geolocation_staging;

IF OBJECT_ID('bronze.order_items_staging', 'U') IS NOT NULL
    DROP TABLE bronze.order_items_staging;

IF OBJECT_ID('bronze.order_payments_staging', 'U') IS NOT NULL
    DROP TABLE bronze.order_payments_staging;

IF OBJECT_ID('bronze.order_reviews_staging', 'U') IS NOT NULL
    DROP TABLE bronze.order_reviews_staging;

IF OBJECT_ID('bronze.orders_staging', 'U') IS NOT NULL
    DROP TABLE bronze.orders_staging;

IF OBJECT_ID('bronze.products_staging', 'U') IS NOT NULL
    DROP TABLE bronze.products_staging;

IF OBJECT_ID('bronze.sellers_staging', 'U') IS NOT NULL
    DROP TABLE bronze.sellers_staging;

IF OBJECT_ID('bronze.product_category_name_translation_staging', 'U') IS NOT NULL
    DROP TABLE bronze.product_category_name_translation_staging;

GO