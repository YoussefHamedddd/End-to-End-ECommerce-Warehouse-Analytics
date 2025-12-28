USE ECommerce_DWH
GO

-- Stored Procedure to orchestrate the loading and transformation of data from Bronze to Silver layer.
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    -- Standard practice to prevent sending row counts back to the client.
    SET NOCOUNT ON;

    -- =============================================================
    -- PART 1: LOAD silver.products (With Imputation and Translation)
    -- =============================================================

    -- Ensure the target table is truncated or dropped/recreated if required by the data lifecycle.
    -- TRUNCATE TABLE silver.products; 

    -- Prepare Unique Translation Table (CTE 1)
    WITH UniqueTranslation AS (
        SELECT 
            T.product_category_name,
            T.product_category_name_english,
            -- Enforce 1-to-1 mapping by assigning a rank
            ROW_NUMBER() OVER (
                PARTITION BY T.product_category_name 
                ORDER BY T.product_category_name_english
            ) AS TranslationRank
        FROM bronze.product_category_name_translation T
    ),

    -- Clean Products and Merge Categories (CTE 2)
    CleanProducts AS (
        SELECT
            P.product_id,
            P.product_name_length,
            P.product_description_length,
            P.product_weight_g,
            P.product_length_cm,
            P.product_height_cm,
            P.product_width_cm,

            -- Deep Cleaning: Remove special chars/spaces, unify case, and prioritize English
            LOWER(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(
                COALESCE(UT.product_category_name_english, P.product_category_name),
                CHAR(160), ''), CHAR(9), ''), CHAR(13), ''), CHAR(10), '')
            )) AS FinalCategoryName
        FROM bronze.products P
        LEFT JOIN UniqueTranslation UT
            ON P.product_category_name = UT.product_category_name
            AND UT.TranslationRank = 1
    ),

    -- Calculate Medians per Category (CTE 3)
    CategoryMedians AS (
        SELECT DISTINCT
            FinalCategoryName,
            -- Calculate median for each dimensional column (Weight, Length, etc.)
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY product_weight_g) OVER (PARTITION BY FinalCategoryName) AS Median_Weight_G,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY product_description_length) OVER (PARTITION BY FinalCategoryName) AS Median_Description_Length,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY product_length_cm) OVER (PARTITION BY FinalCategoryName) AS Median_Length_CM,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY product_height_cm) OVER (PARTITION BY FinalCategoryName) AS Median_Height_CM,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY product_width_cm) OVER (PARTITION BY FinalCategoryName) AS Median_Width_CM
        FROM CleanProducts
    )

    -- 5. Load into SILVER with Median Imputation
    INSERT INTO silver.products (
        product_id,
        product_category_name_english,
        product_name_length,
        product_description_length,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    )
    SELECT
        CP.product_id,
        CP.FinalCategoryName,
        CP.product_name_length,
        
        -- Apply Median Imputation (COALESCE) for missing dimensions
        COALESCE(CP.product_description_length, CM.Median_Description_Length) AS product_description_length,
        COALESCE(CP.product_weight_g, CM.Median_Weight_G) AS product_weight_g,
        COALESCE(CP.product_length_cm, CM.Median_Length_CM) AS product_length_cm,
        COALESCE(CP.product_height_cm, CM.Median_Height_CM) AS product_height_cm,
        COALESCE(CP.product_width_cm, CM.Median_Width_CM) AS product_width_cm

    FROM CleanProducts CP
    LEFT JOIN CategoryMedians CM
        ON CP.FinalCategoryName = CM.FinalCategoryName;

    PRINT '>> SUCCESS: Loaded silver.products with accurate median imputation.';
    
    -----------------------------------------------------
    -- PART 2: LOAD silver.orders (With Chronology Validation)
    -----------------------------------------------------
    
    -- TRUNCATE TABLE silver.orders;

    -- Transformation and Load Logic
    INSERT INTO silver.orders (
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    )
    SELECT
        O.order_id,
        O.customer_id,
        
        -- A. Order Status Standardization: Cleaned, unified (lower), NULLs preserved.
        NULLIF(LOWER(TRIM(O.order_status)), '') AS order_status, 
        
        -- B. Chronology Check (Date Validation)
        O.order_purchase_timestamp, 
        
        -- 2. Approval Date: Must be >= Purchase Date
        CASE 
            WHEN O.order_approved_at >= O.order_purchase_timestamp THEN O.order_approved_at
            ELSE NULL -- Invalid date becomes NULL
        END AS order_approved_at,
        
        -- 3. Carrier Date: Must be >= Approved Date
        CASE
            WHEN O.order_delivered_carrier_date >= O.order_approved_at THEN O.order_delivered_carrier_date
            ELSE NULL -- Invalid date becomes NULL
        END AS order_delivered_carrier_date,
        
        -- 4. Customer Date: Must be >= Carrier Date
        CASE
            WHEN O.order_delivered_customer_date >= O.order_delivered_carrier_date THEN O.order_delivered_customer_date
            ELSE NULL -- Invalid date becomes NULL
        END AS order_delivered_customer_date,
        
        -- 5. Estimated Delivery Date (Loaded As Is)
        O.order_estimated_delivery_date
        
    FROM bronze.orders O;

    PRINT '>> SUCCESS: Loaded silver.orders. Dates are validated, status is standardized, and NULLs are preserved.';

    -----------------------------------------------------
    -- PART 3: LOAD silver.order_items (Core Fact Table)
    -----------------------------------------------------
    
    -- TRUNCATE TABLE silver.order_items;
    
    -- Transformation and Load Logic
    INSERT INTO silver.order_items (
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    )
    SELECT
        I.order_id,
        I.order_item_id,
        I.product_id,
        I.seller_id,
        
        -- Load date as is (relying on DATETIME2 casting for format validation)
        I.shipping_limit_date,
        
        -- Price Validation: Must be >= 0. Set negative values to NULL.
        CASE 
            WHEN I.price IS NOT NULL AND I.price >= 0 THEN I.price
            ELSE NULL 
        END AS price,
        
        -- Freight Validation: Must be >= 0. Set negative values to NULL.
        CASE
            WHEN I.freight_value IS NOT NULL AND I.freight_value >= 0 THEN I.freight_value
            ELSE NULL 
        END AS freight_value
        
    FROM bronze.order_items I;

    PRINT '>> SUCCESS: Loaded silver.order_items. Monetary values are validated.';

    -----------------------------------------------------
    -- PART 4: LOAD silver.order_reviews (CLEANSED DATA)
    -----------------------------------------------------
    
    -- TRUNCATE TABLE silver.order_reviews;

    -- Transformation and Load Logic
    INSERT INTO silver.order_reviews (
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    )
    SELECT
        -- [PROBLEM 1 & 2: TRUNCATION ERROR (Msg 2628)]: 
        -- The Bronze staging table contained extremely long strings (comments) erroneously placed
        -- in the 'review_id' and 'order_id' columns, which were initially defined too short (VARCHAR(50)).
        -- SOLUTION: We expanded the size of these columns in the Silver DDL to VARCHAR(500) to allow loading.
        
        -- [PROBLEM 3: DOUBLE QUOTES]:
        -- IDs contained double quotes (e.g., "ID123") due to the upstream load process failing to strip delimiters.
        -- SOLUTION: TRIM(CHAR(34) FROM ...) is used to remove leading/trailing double quotes (ASCII 34) and spaces.
        TRIM(CHAR(34) FROM T1.review_id),
        TRIM(CHAR(34) FROM T1.order_id), 
        
        -- [PROBLEM 4: DATE CONVERSION ERROR (Msg 241)]:
        -- Direct CAST failed when encountering invalid date strings in the source data.
        -- SOLUTION: TRY_CAST is used to safely convert the score to INT. It returns NULL on failure, preventing the entire batch from failing.
        TRY_CAST(T1.review_score AS INT),
        
        TRIM(T1.review_comment_title), 
        TRIM(T1.review_comment_message),
        
        -- SOLUTION: TRY_CAST is used to safely convert date/time fields. It returns NULL if the string is not a valid date/time format.
        TRY_CAST(T1.review_creation_date AS DATE),
        TRY_CAST(T1.review_answer_timestamp AS DATETIME2(0))
    FROM
        bronze.order_reviews_staging T1;

    PRINT '>> SUCCESS: Loaded silver.order_reviews. Truncation and Quote issues resolved.';
    
    -----------------------------------------------------
    -- PART 5: LOAD silver.product_category_name_translation
    -----------------------------------------------------
    
    -- TRUNCATE TABLE silver.product_category_name_translation;
    
    -- Transformation and Load Logic (Enforce 1-to-1 mapping)
    WITH UniqueTranslation AS (
        SELECT
            T.product_category_name,
            LOWER(TRIM(T.product_category_name_english)) AS Cleaned_English_Name,
            -- Rank ensures only one English translation is selected per Portuguese category
            ROW_NUMBER() OVER (
                PARTITION BY T.product_category_name 
                ORDER BY T.product_category_name_english 
            ) AS TranslationRank
        FROM bronze.product_category_name_translation T
    )
    
    INSERT INTO silver.product_category_name_translation (
        product_category_name,
        product_category_name_english
    )
    SELECT
        UT.product_category_name,
        UT.Cleaned_English_Name
    FROM UniqueTranslation UT
    WHERE UT.TranslationRank = 1;

    PRINT '>> SUCCESS: Loaded silver.product_category_name_translation with 1-to-1 mapping enforced.';
    
    -----------------------------------------------------
    -- PART 6: LOAD silver.sellers
    -----------------------------------------------------
    
    -- TRUNCATE TABLE silver.sellers;
    
    -- Transformation and Load Logic (Geographic Standardization)
    INSERT INTO silver.sellers (
        seller_id,
        seller_city,
        seller_state
    )
    SELECT
        S.seller_id,
        
        -- City Standardization: Clean, unify case, and preserve NULLs.
        NULLIF(LOWER(TRIM(S.seller_city)), '') AS seller_city,
        
        -- State Standardization: Clean, unify case, and preserve NULLs.
        NULLIF(LOWER(TRIM(S.seller_state)), '') AS seller_state
        
    FROM bronze.sellers S;

    PRINT '>> SUCCESS: Loaded silver.sellers. Geographic data standardized.';

    ----------------------------------------------
    -- PART 7: LOAD silver.order_payments
    ----------------------------------------------
    
    -- TRUNCATE TABLE silver.order_payments;

    INSERT INTO silver.order_payments (
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    )
    SELECT
        P.order_id,
        P.payment_sequential,
        
        -- A. Payment Type Standardization: Cleaned, unified (lower), NULLs preserved.
        NULLIF(LOWER(TRIM(P.payment_type)), '') AS payment_type,
        
        -- B. Payment Installments: Loaded as-is.
        P.payment_installments,
        
        -- C. Payment Value Validation: Value must be >= 0. Set negative values to NULL (Data Quality Rule).
        CASE 
            WHEN P.payment_value IS NOT NULL AND P.payment_value >= 0 THEN P.payment_value
            ELSE NULL 
        END AS payment_value
        
    FROM bronze.order_payments P; 

    PRINT '>> SUCCESS: Loaded silver.order_payments. Payment type standardized and monetary value validated.';

    ----------------------------------------------
    -- PART 8: LOAD silver.customers
    ----------------------------------------------

    -- TRUNCATE TABLE silver.customers;

    INSERT INTO silver.customers (
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    )
    SELECT
        C.customer_id,
        C.customer_unique_id,
        
        -- Zip Code Prefix: Loaded as-is.
        C.customer_zip_code_prefix,
        
        -- A. City Standardization: Clean, unify case, and preserve NULLs.
        NULLIF(LOWER(TRIM(C.customer_city)), '') AS customer_city,
        
        -- B. State Standardization: Clean, unify case, and preserve NULLs.
        NULLIF(LOWER(TRIM(C.customer_state)), '') AS customer_state
        
    FROM bronze.customers C; 

    PRINT '>> SUCCESS: Loaded silver.customers. Geographic data standardized.';

    ----------------------------------------------
    -- PART 9: LOAD silver.geolocation
    ----------------------------------------------

    -- TRUNCATE TABLE silver.geolocation;

    INSERT INTO silver.geolocation (
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
    )
    SELECT
        G.geolocation_zip_code_prefix,
        
        -- A. Calculate Average Latitude
        AVG(G.geolocation_lat) AS geolocation_lat,
        
        -- B. Calculate Average Longitude
        AVG(G.geolocation_lng) AS geolocation_lng,
        
        -- C. City Standardization: Choose one representative city name after cleaning
        MAX(NULLIF(LOWER(TRIM(G.geolocation_city)), '')) AS geolocation_city,
        
        -- D. State Standardization: Choose one representative state name
        MAX(NULLIF(LOWER(TRIM(G.geolocation_state)), '')) AS geolocation_state
        
    FROM bronze.geolocation G
    -- *** GROUP ONLY BY THE PRIMARY KEY COLUMN ***
    GROUP BY
        G.geolocation_zip_code_prefix;

    PRINT '>> SUCCESS: Loaded silver.geolocation. Coordinates averaged and Primary Key enforced.';
    
    -- NOTE: Verification Queries at the end are not standard practice in a final stored procedure.
    -- They are commented out but left for reference.
    -- SELECT * FROM silver.geolocation;

END
GO

-- Execute the stored procedure to load data into the Silver layer
exec silver.load_silver


-- chekk data Quality in silver layer 
select * from silver.order_reviews
select * from silver.customers
select * from silver.order_payments
select * from silver.product_category_name_translation
select * from silver.products
select * from silver.geolocation
select * from silver.sellers
select * from silver.orders
select * from silver.order_items

