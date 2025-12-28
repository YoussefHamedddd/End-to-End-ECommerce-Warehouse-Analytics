USE ECommerce_DWH
GO

-----------------------------------------------------
-- 1. Table: silver.products
-----------------------------------------------------
-- Drop table if exists
IF OBJECT_ID('silver.products', 'U') IS NOT NULL
    DROP TABLE silver.products;

-- Create silver.products table
CREATE TABLE silver.products (
    product_id                      VARCHAR(50) PRIMARY KEY,
    product_category_name_english   VARCHAR(100),
    product_name_length             INT,
    product_description_length      INT,
    product_weight_g                INT,
    product_length_cm               INT,
    product_height_cm               INT,
    product_width_cm                INT,
    DWH_create_date                 DATETIME2 DEFAULT GETDATE()
);

-----------------------------------------------------
-- 2. Table: silver.orders
-----------------------------------------------------
-- Drop table if exists
IF OBJECT_ID('silver.orders', 'U') IS NOT NULL
    DROP TABLE silver.orders;

-- Create silver.orders table
CREATE TABLE silver.orders (
    order_id                        VARCHAR(50) PRIMARY KEY,
    customer_id                     VARCHAR(50),
    order_status                    VARCHAR(20),             
    order_purchase_timestamp        DATETIME2,             
    order_approved_at               DATETIME2,             
    order_delivered_carrier_date    DATETIME2,             
    order_delivered_customer_date   DATETIME2,             
    order_estimated_delivery_date   DATETIME2,
    DWH_create_date                 DATETIME2 DEFAULT GETDATE()
);

-----------------------------------------------------
-- 3. Table: silver.order_items (Fact Table)
-----------------------------------------------------
-- Drop table if exists
IF OBJECT_ID('silver.order_items', 'U') IS NOT NULL
    DROP TABLE silver.order_items;

-- Create silver.order_items table
CREATE TABLE silver.order_items (
    order_id                        VARCHAR(50) NOT NULL,
    order_item_id                   INT         NOT NULL,
    product_id                      VARCHAR(50),
    seller_id                       VARCHAR(50),
    shipping_limit_date             DATETIME2,
    price                           DECIMAL(10, 2),
    freight_value                   DECIMAL(10, 2),
    DWH_create_date                 DATETIME2 DEFAULT GETDATE(),
    -- Define Composite Primary Key
    CONSTRAINT PK_order_items PRIMARY KEY (order_id, order_item_id)
);

-----------------------------------------------------
-- 4. Table: silver.product_category_name_translation
-----------------------------------------------------
-- Drop table if exists
IF OBJECT_ID('silver.product_category_name_translation', 'U') IS NOT NULL
    DROP TABLE silver.product_category_name_translation;

-- Create silver.product_category_name_translation table
CREATE TABLE silver.product_category_name_translation (
    product_category_name           VARCHAR(100) PRIMARY KEY, -- Portuguese Category (PK)
    product_category_name_english   VARCHAR(100),             -- Cleaned English Translation
    DWH_create_date                 DATETIME2 DEFAULT GETDATE()
);

-----------------------------------------------------
-- 5. Table: silver.sellers
-----------------------------------------------------
-- Drop table if exists
IF OBJECT_ID('silver.sellers', 'U') IS NOT NULL
    DROP TABLE silver.sellers;

-- Create silver.sellers table
CREATE TABLE silver.sellers (
    seller_id                       VARCHAR(50) PRIMARY KEY,
    seller_city                     VARCHAR(100),
    seller_state                    VARCHAR(50),
    DWH_create_date                 DATETIME2 DEFAULT GETDATE()
);

-----------------------------------------------------
-- 6. Table: silver.order_payments
-----------------------------------------------------
-- Drop table if exists
IF OBJECT_ID('silver.order_payments', 'U') IS NOT NULL
    DROP TABLE silver.order_payments;

-- Create silver.order_payments table
CREATE TABLE silver.order_payments (
    order_id                        VARCHAR(50) NOT NULL,
    payment_sequential              INT         NOT NULL,
    payment_type                    VARCHAR(50),
    payment_installments            INT,
    payment_value                   DECIMAL(10, 2),
    DWH_create_date                 DATETIME2 DEFAULT GETDATE(),
    -- Define Composite Primary Key
    CONSTRAINT PK_order_payments PRIMARY KEY (order_id, payment_sequential)
);

-----------------------------------------------------
-- 7. Table: silver.customers
-----------------------------------------------------
-- Drop table if exists
IF OBJECT_ID('silver.customers', 'U') IS NOT NULL
    DROP TABLE silver.customers;

-- Create silver.customers table
CREATE TABLE silver.customers (
    customer_id                     VARCHAR(50) PRIMARY KEY,
    customer_unique_id              VARCHAR(50),
    customer_zip_code_prefix        INT,
    customer_city                   VARCHAR(100),
    customer_state                  VARCHAR(50),
    DWH_create_date                 DATETIME2 DEFAULT GETDATE()
);

-----------------------------------------------------
-- 8. Table: silver.geolocation
-----------------------------------------------------
-- Drop table if exists
IF OBJECT_ID('silver.geolocation', 'U') IS NOT NULL
    DROP TABLE silver.geolocation;

-- Create silver.geolocation table
CREATE TABLE silver.geolocation (
    geolocation_zip_code_prefix     INT PRIMARY KEY, -- Grouping is applied on this column in the ETL
    geolocation_lat                 DECIMAL(15, 8),
    geolocation_lng                 DECIMAL(15, 8),
    geolocation_city                VARCHAR(100),
    geolocation_state               VARCHAR(50),
    DWH_create_date                 DATETIME2 DEFAULT GETDATE()
);

-----------------------------------------------------
-- 9. Table: silver.order_reviews (The Final, Functional Definition)
-----------------------------------------------------
-- Drop table if exists
IF OBJECT_ID('silver.order_reviews', 'U') IS NOT NULL
    DROP TABLE silver.order_reviews;

-- Create silver.order_reviews table (using expanded sizes to resolve truncation errors)
CREATE TABLE silver.order_reviews (
    -- Expanded size to 500 characters to temporarily hold long string/comment data
    -- that was erroneously loaded into this column from the Bronze layer.
    review_id                       VARCHAR(500), 
    
    -- Expanded size to 500 characters for the same reason (erroneous long string loading).
    order_id                        VARCHAR(500), 
    
    review_score                    INT,
    review_comment_title            NVARCHAR(500), -- Expanded size for long titles
    review_comment_message          NVARCHAR(MAX),
    review_creation_date            DATE,          -- Converted from text
    review_answer_timestamp         DATETIME2(0),  -- Converted from text
    DWH_create_date                 DATETIME2 DEFAULT GETDATE()
    
  
);
GO