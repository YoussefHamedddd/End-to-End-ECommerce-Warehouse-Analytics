/* ===============================================================================
PHASE 1: STANDARDIZING DATA TYPES & REBUILDING PRIMARY KEYS
===============================================================================
*/

-- 1. Ensure Order_ID data type and length are consistent (VARCHAR 150 for safety)
ALTER TABLE Gold.Fact_Order_Header ALTER COLUMN Order_ID VARCHAR(150) NOT NULL;
GO

-- 2. Drop any legacy Primary Key constraints to prevent conflicts during reconstruction
DECLARE @sql NVARCHAR(MAX) = '';
SELECT @sql += 'ALTER TABLE Gold.Fact_Order_Header DROP CONSTRAINT ' + name + ';'
FROM sys.key_constraints 
WHERE parent_object_id = OBJECT_ID('Gold.Fact_Order_Header') AND type = 'PK';
EXEC sp_executesql @sql;
GO

-- 3. Re-establish the clean Primary Key for the Order Header table
ALTER TABLE Gold.Fact_Order_Header ADD CONSTRAINT PK_Fact_Order_Header PRIMARY KEY (Order_ID);
GO

-- 4. Unify Order_ID data types across all Fact tables to ensure compatible Joins
ALTER TABLE Gold.Fact_Order_Reviews ALTER COLUMN Order_ID VARCHAR(150) NOT NULL;
ALTER TABLE Gold.Fact_Geolocation     ALTER COLUMN Order_ID VARCHAR(150) NOT NULL;
ALTER TABLE Gold.Fact_Sales           ALTER COLUMN Order_ID VARCHAR(150) NOT NULL;
GO

/* ===============================================================================
PHASE 2: ENFORCING RELATIONSHIPS (FOREIGN KEYS)
===============================================================================
*/

-- Apply Foreign Key constraints using WITH NOCHECK to bypass existing orphaned records
ALTER TABLE Gold.Fact_Order_Reviews WITH NOCHECK 
ADD CONSTRAINT FK_Reviews_Order FOREIGN KEY (Order_ID) REFERENCES Gold.Fact_Order_Header(Order_ID);

ALTER TABLE Gold.Fact_Geolocation WITH NOCHECK 
ADD CONSTRAINT FK_Geo_Order FOREIGN KEY (Order_ID) REFERENCES Gold.Fact_Order_Header(Order_ID);

ALTER TABLE Gold.Fact_Sales WITH NOCHECK 
ADD CONSTRAINT FK_Sales_Order_Link FOREIGN KEY (Order_ID) REFERENCES Gold.Fact_Order_Header(Order_ID);

PRINT '--- [CRITICAL SUCCESS] All Constraints Forced Successfully! ---';

/* ===============================================================================
PHASE 3: PREPARING DIMENSION TABLES (GEOLOCATION & PAYMENTS)
===============================================================================
*/

-- 1. Setup Geolocation Dimension Primary Key
ALTER TABLE Gold.Dim_Geolocation ALTER COLUMN Geolocation_Key INT NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('Gold.Dim_Geolocation') AND is_primary_key = 1)
    ALTER TABLE Gold.Dim_Geolocation ADD CONSTRAINT PK_Dim_Geolocation PRIMARY KEY (Geolocation_Key);

-- 2. Setup Payment Type Dimension Primary Key
ALTER TABLE Gold.Dim_Payment_Type ALTER COLUMN Payment_Key INT NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('Gold.Dim_Payment_Type') AND is_primary_key = 1)
    ALTER TABLE Gold.Dim_Payment_Type ADD CONSTRAINT PK_Dim_Payment_Type PRIMARY KEY (Payment_Key);

/* ===============================================================================
PHASE 4: LINKING ORPHANED TABLES (STRENGTHENING STAR SCHEMA)
===============================================================================
*/

-- Ensure FK columns exist in Dimension and Fact tables
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Gold.Dim_Customer') AND name = 'Geolocation_Key')
    ALTER TABLE Gold.Dim_Customer ADD Geolocation_Key INT;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Gold.Dim_Seller') AND name = 'Geolocation_Key')
    ALTER TABLE Gold.Dim_Seller ADD Geolocation_Key INT;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('Gold.Fact_Sales') AND name = 'Payment_Key')
    ALTER TABLE Gold.Fact_Sales ADD Payment_Key INT;
GO

-- Apply Foreign Keys to connect Customers and Sellers to Geolocation
ALTER TABLE Gold.Dim_Customer WITH NOCHECK 
ADD CONSTRAINT FK_Customer_Geo FOREIGN KEY (Geolocation_Key) REFERENCES Gold.Dim_Geolocation(Geolocation_Key);

ALTER TABLE Gold.Dim_Seller WITH NOCHECK 
ADD CONSTRAINT FK_Seller_Geo FOREIGN KEY (Geolocation_Key) REFERENCES Gold.Dim_Geolocation(Geolocation_Key);

-- Connect Fact_Sales to Payment Dimension
ALTER TABLE Gold.Fact_Sales WITH NOCHECK 
ADD CONSTRAINT FK_Sales_Payment FOREIGN KEY (Payment_Key) REFERENCES Gold.Dim_Payment_Type(Payment_Key);

PRINT '--- [SUCCESS] All Orphan Tables are now Linked! ---';

/* ===============================================================================
PHASE 5: DATA POPULATION & GEOSPATIAL RECOVERY
===============================================================================
*/

-- Update Sales table with Payment Keys by joining the original Payment staging data
UPDATE FS
SET FS.Payment_Key = PT.Payment_Key
FROM Gold.Fact_Sales FS
INNER JOIN silver.order_payments SOP ON FS.Order_ID = SOP.order_id
INNER JOIN Gold.Dim_Payment_Type PT ON SOP.payment_type = PT.payment_type;

-- Expand Geolocation columns to prevent text truncation during inserts
ALTER TABLE Gold.Dim_Geolocation ALTER COLUMN geolocation_state VARCHAR(255);
ALTER TABLE Gold.Dim_Geolocation ALTER COLUMN geolocation_city VARCHAR(255);

-- GEOSPATIAL RECOVERY: Insert missing cities from Customer/Seller data into the Geo Dimension
INSERT INTO Gold.Dim_Geolocation (geolocation_zip_code_prefix, geolocation_city, geolocation_state)
SELECT DISTINCT 0, customer_city, customer_state
FROM Gold.Dim_Customer C
WHERE NOT EXISTS (
    SELECT 1 FROM Gold.Dim_Geolocation G 
    WHERE G.geolocation_city = C.customer_city AND G.geolocation_state = C.customer_state
);

-- Synchronize Customer/Seller Geolocation_Keys based on City/State matching
UPDATE C
SET C.Geolocation_Key = G.Geolocation_Key
FROM Gold.Dim_Customer C
INNER JOIN Gold.Dim_Geolocation G ON C.customer_city = G.geolocation_city AND C.customer_state = G.geolocation_state;

/* ===============================================================================
PHASE 6: FINAL SCHEMA REFINEMENT & DATA VALIDATION
===============================================================================
*/

-- Handle NULL or missing Product references by inserting an 'UNKNOWN' member
SET IDENTITY_INSERT gold.Dim_Product ON;
INSERT INTO gold.Dim_Product (Product_Key, product_id, product_category_name)
VALUES (-1, 'UNKNOWN', 'Unknown');
SET IDENTITY_INSERT gold.Dim_Product OFF;

-- Assign orphaned Review records to the 'UNKNOWN' product to maintain Referential Integrity
UPDATE gold.Fact_Order_Reviews
SET Product_Key = -1
WHERE Product_Key NOT IN (SELECT Product_Key FROM gold.Dim_Product);

-- Final Foreign Key enforcement for Product Dimension
ALTER TABLE gold.Fact_Order_Reviews
ADD CONSTRAINT FK_FactOrderReviews_DimProduct
FOREIGN KEY (Product_Key) REFERENCES gold.Dim_Product (Product_Key);