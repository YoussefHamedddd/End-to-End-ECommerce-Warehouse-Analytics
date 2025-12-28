CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON; -- Crucial for professional stored procedures
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @rows_affected INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT '>> START: Loading Bronze Layer (Professional Hardening)';
        PRINT '================================================';

        -- NOTE: BULK INSERTs are included to show the process flow, but must be executed
        -- via a dedicated job or with specific server permissions.

        ---------------------------------------------------------------------------------
        -- 1. CUSTOMERS TABLE (Hardening: ID Cleaning & TRY_CONVERT)
        ---------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Processing Table: customers';

        TRUNCATE TABLE bronze.customers;
        TRUNCATE TABLE bronze.customers_staging;
        
        -- Staging Load (BULK INSERT)
        PRINT '>> Inserting Data Into: bronze.customers_staging (BULK INSERT)';
        BULK INSERT bronze.customers_staging
        FROM 'C:\Users\dell\Desktop\DWH\E Commerce DWH\Data\olist_customers_dataset.csv'
        WITH (
              FIRSTROW = 2, -- Changed from 3 to 2, assuming only header skip is needed after fixing row 3 manually/defensively
              FIELDTERMINATOR = ',',
              FIELDQUOTE = '"',
              ROWTERMINATOR = '0x0a',
              CODEPAGE = '65001',
              TABLOCK
            );

        PRINT '>> Transferring Data with Safe Conversion to bronze.customers...';
        INSERT INTO bronze.customers (
            customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
        )
        SELECT
            -- FIX: Apply TRIM(REPLACE) for all IDs
            TRIM(REPLACE(customer_id, '"', '')),
            TRIM(REPLACE(customer_unique_id, '"', '')),
            -- FIX: Use TRY_CONVERT for safe conversion of zip code
            TRY_CONVERT(INT, NULLIF(TRIM(REPLACE(customer_zip_code_prefix, '"', '')), '')), 
            customer_city,
            customer_state
        FROM bronze.customers_staging;

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.customers: ' + CAST(@rows_affected AS NVARCHAR) + ' rows. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ------------------------------------------------';


        ---------------------------------------------------------------------------------
        -- 2. GEOLOCATION TABLE (Hardening: TRY_CONVERT)
        ---------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Processing Table: geolocation';

        TRUNCATE TABLE bronze.geolocation;
        TRUNCATE TABLE bronze.geolocation_staging;

        -- Staging Load (BULK INSERT)
        PRINT '>> Inserting Data Into: bronze.geolocation_staging (BULK INSERT)';
        BULK INSERT bronze.geolocation_staging
        FROM 'C:\Users\dell\Desktop\DWH\E Commerce DWH\Data\olist_geolocation_dataset.csv'
          WITH (
              FIRSTROW = 2,
              FIELDTERMINATOR = ',',
              FIELDQUOTE = '"',
              ROWTERMINATOR = '0x0a',
              CODEPAGE = '65001',
              TABLOCK
            );

        PRINT '>> Transferring Data with Safe Conversion to bronze.geolocation...';
        INSERT INTO bronze.geolocation (
            geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state
        )
        SELECT
            -- Use TRY_CONVERT for all numeric fields
            TRY_CONVERT(INT, NULLIF(TRIM(REPLACE(geolocation_zip_code_prefix, '"', '')), '')), 
            TRY_CONVERT(DECIMAL(10, 8), NULLIF(TRIM(geolocation_lat), '')), 
            TRY_CONVERT(DECIMAL(10, 8), NULLIF(TRIM(geolocation_lng), '')),
            geolocation_city,
            geolocation_state
        FROM bronze.geolocation_staging;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.geolocation: ' + CAST(@rows_affected AS NVARCHAR) + ' rows. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ------------------------------------------------';


        ---------------------------------------------------------------------------------
        -- 3. ORDER ITEMS TABLE (Hardening: ID Cleaning & TRY_CONVERT)
        ---------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Processing Table: order_items';

        TRUNCATE TABLE bronze.order_items;
        TRUNCATE TABLE bronze.order_items_staging;

        -- Staging Load (BULK INSERT)
        PRINT '>> Inserting Data Into: bronze.order_items_staging (BULK INSERT)';
        BULK INSERT bronze.order_items_staging
        FROM 'C:\Users\dell\Desktop\DWH\E Commerce DWH\Data\olist_order_items_dataset.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            FIELDQUOTE = '"',
            ROWTERMINATOR = '0x0a',
            CODEPAGE = '65001',
            TABLOCK
        );

        PRINT '>> Transferring Data with Safe Conversion to bronze.order_items...';
        INSERT INTO bronze.order_items (
            order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value
        )
        SELECT
            -- FIX: Apply TRIM(REPLACE) for all IDs
            TRIM(REPLACE(order_id, '"', '')),
            TRY_CONVERT(INT, NULLIF(TRIM(order_item_id), '')), -- order_item_id is typically INT
            TRIM(REPLACE(product_id, '"', '')),
            TRIM(REPLACE(seller_id, '"', '')),
            TRY_CONVERT(DATETIME2, NULLIF(TRIM(shipping_limit_date), '')), -- Use DATETIME2 for precision
            TRY_CONVERT(DECIMAL(10, 2), NULLIF(TRIM(price), '')), -- Use DECIMAL for monetary values
            TRY_CONVERT(DECIMAL(10, 2), NULLIF(TRIM(freight_value), ''))
        FROM bronze.order_items_staging;

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.order_items: ' + CAST(@rows_affected AS NVARCHAR) + ' rows. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ------------------------------------------------';

        
        ---------------------------------------------------------------------------------
        -- 4. ORDER REVIEWS TABLE (Hardening: ID Cleaning & TRY_CONVERT)
        ---------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Processing Table: order_reviews';

        TRUNCATE TABLE bronze.order_reviews;
        TRUNCATE TABLE bronze.order_reviews_staging;

        -- Staging Load (BULK INSERT)
        PRINT '>> Inserting Data Into: bronze.order_reviews_staging (BULK INSERT)';
        BULK INSERT bronze.order_reviews_staging
        FROM 'C:\Users\dell\Desktop\DWH\E Commerce DWH\Data\olist_order_reviews_dataset.csv'
        WITH (
            FIRSTROW = 2, FIELDTERMINATOR = ',', FIELDQUOTE = '"', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK
        );

        PRINT '>> Transferring Data with Safe Conversion to bronze.order_reviews...';
        INSERT INTO bronze.order_reviews (
            review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp
        )
        SELECT
            -- FIX: Apply TRIM(REPLACE) for all IDs
            TRIM(REPLACE(review_id, '"', '')),
            TRIM(REPLACE(order_id, '"', '')),
            TRY_CONVERT(INT, NULLIF(TRIM(review_score), '')), 
            review_comment_title,
            review_comment_message,
            -- Use TRY_CONVERT for all datetime fields
            TRY_CONVERT(DATETIME2, NULLIF(TRIM(review_creation_date), '')), 
            TRY_CONVERT(DATETIME2, NULLIF(TRIM(review_answer_timestamp), ''))
        FROM bronze.order_reviews_staging;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.order_reviews: ' + CAST(@rows_affected AS NVARCHAR) + ' rows. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ------------------------------------------------';


        ---------------------------------------------------------------------------------
        -- 5. ORDERS TABLE (Hardening: ID Cleaning & TRY_CONVERT)
        ---------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Processing Table: orders';

        TRUNCATE TABLE bronze.orders;
        TRUNCATE TABLE bronze.orders_staging;

        -- Staging Load (BULK INSERT)
        PRINT '>> Inserting Data Into: bronze.orders_staging (BULK INSERT)';
        BULK INSERT bronze.orders_staging
        FROM 'C:\Users\dell\Desktop\DWH\E Commerce DWH\Data\olist_orders_dataset.csv'
        WITH (
            FIRSTROW = 2, FIELDTERMINATOR = ',', FIELDQUOTE = '"', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK
        );

        PRINT '>> Transferring Data with Safe Conversion to bronze.orders...';
        INSERT INTO bronze.orders (
            order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
        )
        SELECT
            -- FIX: Apply TRIM(REPLACE) for all IDs
            TRIM(REPLACE(order_id, '"', '')),
            TRIM(REPLACE(customer_id, '"', '')),
            order_status,
            -- Use TRY_CONVERT for all datetime fields
            TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_purchase_timestamp), '')),
            TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_approved_at), '')),
            TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_delivered_carrier_date), '')),
            TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_delivered_customer_date), '')),
            TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_estimated_delivery_date), ''))
        FROM bronze.orders_staging;

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.orders: ' + CAST(@rows_affected AS NVARCHAR) + ' rows. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ------------------------------------------------';


        ---------------------------------------------------------------------------------
        -- 6. PRODUCTS TABLE (Hardening: ID Cleaning & TRY_CONVERT)
        ---------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Processing Table: products';

        TRUNCATE TABLE bronze.products;
        TRUNCATE TABLE bronze.products_staging;

        -- Staging Load (BULK INSERT)
        PRINT '>> Inserting Data Into: bronze.products_staging (BULK INSERT)';
        BULK INSERT bronze.products_staging
        FROM 'C:\Users\dell\Desktop\DWH\E Commerce DWH\Data\olist_products_dataset.csv'
        WITH (
            FIRSTROW = 2, FIELDTERMINATOR = ',', FIELDQUOTE = '"', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK
        );

        PRINT '>> Transferring Data with Safe Conversion to bronze.products...';
        INSERT INTO bronze.products (
            product_id, product_category_name, product_name_length, product_description_length, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
        )
        SELECT
            -- FIX: Apply TRIM(REPLACE) for the Product ID
            TRIM(REPLACE(product_id, '"', '')),
            product_category_name,
            -- Use TRY_CONVERT for all dimensional metrics
            TRY_CONVERT(INT, NULLIF(TRIM(product_name_length), '')), 
            TRY_CONVERT(INT, NULLIF(TRIM(product_description_length), '')), 
            TRY_CONVERT(INT, NULLIF(TRIM(product_photos_qty), '')),
            TRY_CONVERT(INT, NULLIF(TRIM(product_weight_g), '')),
            TRY_CONVERT(INT, NULLIF(TRIM(product_length_cm), '')),
            TRY_CONVERT(INT, NULLIF(TRIM(product_height_cm), '')),
            TRY_CONVERT(INT, NULLIF(TRIM(product_width_cm), ''))
        FROM bronze.products_staging;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.products: ' + CAST(@rows_affected AS NVARCHAR) + ' rows. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ------------------------------------------------';


        ---------------------------------------------------------------------------------
        -- 7. SELLERS TABLE (Hardening: ID Cleaning & TRY_CONVERT)
        ---------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Processing Table: sellers';

        TRUNCATE TABLE bronze.sellers;
        TRUNCATE TABLE bronze.sellers_staging;

        -- Staging Load (BULK INSERT)
        PRINT '>> Inserting Data Into: bronze.sellers_staging (BULK INSERT)';
        BULK INSERT bronze.sellers_staging
        FROM 'C:\Users\dell\Desktop\DWH\E Commerce DWH\Data\olist_sellers_dataset.csv'
        WITH (
            FIRSTROW = 2, FIELDTERMINATOR = ',', FIELDQUOTE = '"', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK
        );

        PRINT '>> Transferring Data with Safe Conversion to bronze.sellers...';
        INSERT INTO bronze.sellers (
            seller_id, seller_zip_code_prefix, seller_city, seller_state
        )
        SELECT
            -- FIX: Apply TRIM(REPLACE) for the Seller ID
            TRIM(REPLACE(seller_id, '"', '')),
            -- Use TRY_CONVERT for zip code
            TRY_CONVERT(INT, NULLIF(TRIM(REPLACE(seller_zip_code_prefix, '"', '')), '')),
            seller_city,
            seller_state
        FROM bronze.sellers_staging;

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.sellers: ' + CAST(@rows_affected AS NVARCHAR) + ' rows. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ------------------------------------------------';


        ---------------------------------------------------------------------------------
        -- 8. PRODUCT CATEGORY NAME TRANSLATION TABLE (No Conversion Needed)
        ---------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Processing Table: product_category_name_translation';

        TRUNCATE TABLE bronze.product_category_name_translation;
        TRUNCATE TABLE bronze.product_category_name_translation_staging;

        -- Staging Load (BULK INSERT)
        PRINT '>> Inserting Data Into: bronze.product_category_name_translation_staging (BULK INSERT)';
        BULK INSERT bronze.product_category_name_translation_staging
        FROM 'C:\Users\dell\Desktop\DWH\E Commerce DWH\Data\product_category_name_translation.csv'
        WITH (
            FIRSTROW = 2, FIELDTERMINATOR = ',', FIELDQUOTE = '"', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK
        );

        PRINT '>> Transferring Data to bronze.product_category_name_translation...';
        INSERT INTO bronze.product_category_name_translation (
            product_category_name, product_category_name_english
        )
        SELECT
            TRIM(REPLACE(product_category_name, '"', '')), -- Cleaning the category ID just in case
            product_category_name_english
        FROM bronze.product_category_name_translation_staging; 
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.product_category_name_translation: ' + CAST(@rows_affected AS NVARCHAR) + ' rows. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ------------------------------------------------';



		----------- 9- ORDER PAYEMNET ----------------------

		SET @start_time = GETDATE();
        PRINT '>> Processing Table: order_payments';

        TRUNCATE TABLE bronze.order_payments;
        TRUNCATE TABLE bronze.order_payments_staging;

        PRINT '>> Inserting Data Into: bronze.order_payments_staging (BULK INSERT)';
        BULK INSERT bronze.order_payments_staging
        FROM 'C:\Users\dell\Desktop\DWH\E Commerce DWH\Data\olist_order_payments_dataset.csv'
         WITH (
            FIRSTROW = 2, FIELDTERMINATOR = ',', FIELDQUOTE = '"', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK
        );

        PRINT '>> Transferring Data with Safe Conversion to bronze.order_payments...';
        INSERT INTO bronze.order_payments (
            order_id, payment_sequential, payment_type, payment_installments, payment_value
        )
        SELECT
            TRIM(REPLACE(REPLACE(order_id, '"', ''), CHAR(13), '')),
            TRY_CONVERT(INT, NULLIF(TRIM(payment_sequential), '')),
            payment_type,
            TRY_CONVERT(INT, NULLIF(TRIM(payment_installments), '')),
            TRY_CONVERT(DECIMAL(10, 2), payment_value)
        FROM bronze.order_payments_staging;

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.order_payments: ' + CAST(@rows_affected AS NVARCHAR) + ' rows. Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        


		PRINT '>> ------------------------------------------------- ' ;


        -- =================================================================
        -- AUDIT AND COMPLETION
        -- =================================================================
        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT '>> SUCCESS: Bronze Layer Loading Completed.';
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        -- Error Logging Section
        PRINT '================================================';
        PRINT '>> ERROR OCCURRED DURING BRONZE LAYER LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
        -- Professional practice suggests raising an error to stop execution
        THROW;
    END CATCH
END

EXEC bronze.load_bronze


SELECT * FROM bronze.customers
SELECT * FROM bronze.geolocation
SELECT * FROM bronze.order_items
SELECT * FROM bronze.order_payments
SELECT * FROM bronze.order_reviews
SELECT * FROM bronze.orders
SELECT * FROM bronze.sellers
SELECT * FROM bronze.product_category_name_translation
SELECT * FROM bronze.products