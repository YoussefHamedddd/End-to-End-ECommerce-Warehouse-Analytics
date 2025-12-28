CREATE or ALTER PROCEDURE Gold.Load_Gold
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '--- [START] ETL Process: Silver to Gold ---';

    -------------------------------------------------------
    -- 1. Data Refresh (Full Reload Strategy)
    -------------------------------------------------------
    -- Truncate Fact tables first to handle dependency order
    TRUNCATE TABLE Gold.Fact_Sales;
    TRUNCATE TABLE Gold.Fact_Order_Header;
    TRUNCATE TABLE Gold.Fact_Order_Reviews;
    TRUNCATE TABLE Gold.Fact_Geolocation;
    
    -- Delete Dimension data (using DELETE to maintain referential integrity if FKs exist)
    DELETE FROM Gold.Dim_Customer;
    DELETE FROM Gold.Dim_Product;
    DELETE FROM Gold.Dim_Seller;
    DELETE FROM Gold.Dim_Payment_Type;
    DELETE FROM Gold.Dim_Time;
    DELETE FROM Gold.Dim_Geolocation;

    -------------------------------------------------------
    -- 2. Dimension Loading (Master Data)
    -------------------------------------------------------

    -- Populate Dim_Customer (Uniqueness enforced via DISTINCT)
    INSERT INTO Gold.Dim_Customer (customer_id, customer_unique_id, customer_city, customer_state)
    SELECT DISTINCT customer_id, customer_unique_id, customer_city, customer_state FROM silver.customers;

    -- Populate Dim_Product (Merging English categories from Silver)
    INSERT INTO Gold.Dim_Product (product_id, product_category_name, product_name_length, product_description_length, 
                                 product_weight_g, product_length_cm, product_height_cm, product_width_cm)
    SELECT product_id, product_category_name_english, product_name_length, product_description_length, 
           product_weight_g, product_length_cm, product_height_cm, product_width_cm FROM silver.products;

    -- Populate Dim_Seller
    INSERT INTO Gold.Dim_Seller (seller_id, seller_city, seller_state)
    SELECT DISTINCT seller_id, seller_city, seller_state FROM silver.sellers;

    -- Populate Dim_Payment_Type (Logical Grouping: Card vs Other)
    INSERT INTO Gold.Dim_Payment_Type (payment_type, payment_type_group)
    SELECT DISTINCT payment_type, 
           CASE WHEN payment_type IN ('credit_card', 'debit_card') THEN 'Card Payment' ELSE 'Other' END
    FROM silver.order_payments;

    -- Populate Dim_Geolocation (Standardizing text and zip codes)
    PRINT '--- Loading Dim_Geolocation ---';
    INSERT INTO Gold.Dim_Geolocation (geolocation_zip_code_prefix, geolocation_city, geolocation_state)
    SELECT DISTINCT 
        LEFT(TRIM(CAST(geolocation_zip_code_prefix AS VARCHAR(20))), 10),
        LEFT(LOWER(TRIM(geolocation_city)), 100),
        CAST(LOWER(TRIM(geolocation_state)) AS VARCHAR(20))
    FROM silver.geolocation;

    -- Populate Dim_Time (Dynamic Calendar Generation for 2016-2018)
    DECLARE @Start DATE = '2016-01-01', @End DATE = '2018-12-31';
    WHILE @Start <= @End
    BEGIN
        INSERT INTO Gold.Dim_Time (Time_Key, Full_Date, Calendar_Year, Calendar_Quarter, Calendar_Month, Day_of_Month, Day_of_Week, Week_of_Year, Is_Weekend)
        SELECT 
            CONVERT(INT, CONVERT(VARCHAR(8), @Start, 112)), -- Smart Key (YYYYMMDD)
            @Start, YEAR(@Start), DATEPART(QQ, @Start),
            MONTH(@Start), DAY(@Start), DATENAME(DW, @Start), DATEPART(WK, @Start),
            CASE WHEN DATENAME(DW, @Start) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END;
        SET @Start = DATEADD(DAY, 1, @Start);
    END;

    -------------------------------------------------------
    -- 3. Fact Table Loading (Transactions & Metrics)
    -------------------------------------------------------

    -- Populate Fact_Order_Header (High-level order metrics)
    INSERT INTO Gold.Fact_Order_Header (Customer_Key, Purchase_Time_Key, Approved_Time_Key, Order_ID, Order_Status, order_purchase_timestamp, order_approved_at)
    SELECT ISNULL(dc.Customer_Key, 0), CONVERT(INT, CONVERT(VARCHAR(8), o.order_purchase_timestamp, 112)),
           ISNULL(CONVERT(INT, CONVERT(VARCHAR(8), o.order_approved_at, 112)), 0), o.order_id, o.order_status, o.order_purchase_timestamp, o.order_approved_at
    FROM silver.orders o 
    LEFT JOIN Gold.Dim_Customer dc ON o.customer_id = dc.customer_id;

    -- Populate Fact_Sales (Granular line-item sales data)
    INSERT INTO Gold.Fact_Sales (Order_ID, Order_Item_ID, Customer_Key, Product_Key, Seller_Key, Order_Purchase_Time_Key, Order_Estimated_Time_Key, Order_Delivered_Time_Key, Price, Freight_Value)
    SELECT oi.order_id, oi.order_item_id, ISNULL(dc.Customer_Key, 0), ISNULL(dp.Product_Key, 0), ISNULL(ds.Seller_Key, 0),
           CONVERT(INT, CONVERT(VARCHAR(8), o.order_purchase_timestamp, 112)),
           CONVERT(INT, CONVERT(VARCHAR(8), o.order_estimated_delivery_date, 112)),
           ISNULL(CONVERT(INT, CONVERT(VARCHAR(8), o.order_delivered_customer_date, 112)), 0), oi.price, oi.freight_value
    FROM silver.order_items oi
    JOIN silver.orders o ON oi.order_id = o.order_id
    LEFT JOIN Gold.Dim_Customer dc ON o.customer_id = dc.customer_id
    LEFT JOIN Gold.Dim_Product dp ON oi.product_id = dp.product_id
    LEFT JOIN Gold.Dim_Seller ds ON oi.seller_id = ds.seller_id;

    -- Populate Fact_Order_Reviews (Sentiment Analysis)
    INSERT INTO Gold.Fact_Order_Reviews (
        Review_ID, Order_ID, Customer_Key, Product_Key, 
        Review_Creation_Time_Key, Review_Score, Review_Comment_Length
    )
    SELECT 
        CAST(r.review_id AS VARCHAR(150)), 
        CAST(r.order_id AS VARCHAR(150)),
        ISNULL(dc.Customer_Key, 0),
        0, -- Product_Key placeholder (updated in next step)
        ISNULL(CONVERT(INT, CONVERT(VARCHAR(8), r.review_creation_date, 112)), 0),
        ISNULL(r.review_score, 0),
        LEN(ISNULL(r.review_comment_message, ''))
    FROM silver.order_reviews r
    JOIN silver.orders o ON r.order_id = o.order_id
    LEFT JOIN Gold.Dim_Customer dc ON o.customer_id = dc.customer_id;

    PRINT '--- [SUCCESS] Gold Layer Loaded Successfully! ---';



-- Populate Fact_Geolocation (Logic: Using Customer Zip Code as a Proxy for Seller location where missing)
    INSERT INTO Gold.Fact_Geolocation (
        Order_ID, 
        Customer_Geolocation_Key, 
        Seller_Geolocation_Key, 
        Order_Purchase_Time_Key, 
        Order_Delivered_Time_Key, 
        Total_Freight_Value, 
        Delivery_Time_Days
    )
    SELECT 
        o.order_id, 
        ISNULL(dgc.Geolocation_Key, 0), -- Map Customer Geolocation
        ISNULL(dgc.Geolocation_Key, 0), -- Proxy: Using Customer Loc for Seller (Data Quality Workaround)
        CONVERT(INT, CONVERT(VARCHAR(8), o.order_purchase_timestamp, 112)),
        ISNULL(CONVERT(INT, CONVERT(VARCHAR(8), o.order_delivered_customer_date, 112)), 0),
        SUM(oi.freight_value),          -- Aggregating total shipping cost per order
        DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) -- Calculate Lead Time
    FROM silver.orders o
    JOIN silver.order_items oi ON o.order_id = oi.order_id
    JOIN silver.customers c ON o.customer_id = c.customer_id
    LEFT JOIN Gold.Dim_Geolocation dgc ON c.customer_zip_code_prefix = dgc.geolocation_zip_code_prefix
    GROUP BY o.order_id, dgc.Geolocation_Key, o.order_purchase_timestamp, o.order_delivered_customer_date;

    PRINT '--- [SUCCESS] Gold Layer Loaded Successfully! ---';

END;
GO






-------------------------------------------------------
-- 4. Post-Load Cleanup & Imputation
-------------------------------------------------------
CREATE OR ALTER PROCEDURE Gold.clean_dim_product
AS
BEGIN
    SET NOCOUNT ON;
    
    PRINT '--- Starting Imputation for Dim_Product Missing Values ---';

    -- Handling Null Categories
    UPDATE Gold.Dim_Product SET product_category_name = 'Uncategorized' WHERE product_category_name IS NULL;

    -- Using Statistical Imputation (Mean/Average) for missing product measurements
    DECLARE @AvgNameLength INT, @AvgDescLength INT;
    
    SELECT @AvgNameLength = AVG(product_name_length) FROM Gold.Dim_Product WHERE product_name_length IS NOT NULL; 
    UPDATE Gold.Dim_Product SET product_name_length = @AvgNameLength WHERE product_name_length IS NULL;

    SELECT @AvgDescLength = AVG(product_description_length) FROM Gold.Dim_Product WHERE product_description_length IS NOT NULL;
    UPDATE Gold.Dim_Product SET product_description_length = @AvgDescLength WHERE product_description_length IS NULL;

    PRINT '--- Cleanup for Gold.Dim_Product Completed Successfully ---';
END;
GO