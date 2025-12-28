-- Dim_Customer Definition (DDL)
IF OBJECT_ID('Gold.Dim_Customer', 'U') IS NOT NULL DROP TABLE Gold.Dim_Customer;
CREATE TABLE Gold.Dim_Customer (
    Customer_Key        INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key (Auto-incrementing PK)
    customer_id         VARCHAR(50) NOT NULL,          -- Business Key (Original Source ID)
    customer_unique_id  VARCHAR(50),                   -- Unique Customer Identifier
    customer_city       VARCHAR(100),                  -- Customer City
    customer_state      VARCHAR(50),                   -- Customer State (Province)
    DWH_Start_Date      DATETIME2 NOT NULL DEFAULT GETDATE(), -- Row entry timestamp
    Is_Current          BIT DEFAULT 1                  -- Flag for Slowly Changing Dimension (SCD)
);

-- Fact_Order_Header Definition (DDL)
IF OBJECT_ID('Gold.Fact_Order_Header', 'U') IS NOT NULL DROP TABLE Gold.Fact_Order_Header;
CREATE TABLE Gold.Fact_Order_Header (
    Order_Header_SK         INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key for Order Header
    
    -- Foreign Keys (Linking to Dimensions)
    Customer_Key            INT NOT NULL,                  -- Reference to Dim_Customer
    
    -- Time Dimension Keys (FKs to Dim_Time)
    Purchase_Time_Key       INT NOT NULL,                  -- FK to Dim_Time (Purchase Date)
    Approved_Time_Key       INT,                           -- FK to Dim_Time (Approval Date)
    
    -- Operational Attributes
    Order_ID                VARCHAR(50) NOT NULL,          -- Business Order ID
    Order_Status            VARCHAR(20),                   -- Current status of the order
    
    -- Base Timestamps for Calculation
    order_purchase_timestamp DATETIME2,
    order_approved_at       DATETIME2,
    
    -- Derived Metrics (Calculated Column)
    Time_to_Approve_Hours   AS (DATEDIFF(HOUR, order_purchase_timestamp, order_approved_at))
);

USE ECommerce_DWH
GO

-- Dim_Product Definition (DDL)
IF OBJECT_ID('Gold.Dim_Product', 'U') IS NOT NULL DROP TABLE Gold.Dim_Product;
CREATE TABLE Gold.Dim_Product (
    Product_Key             INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    product_id              VARCHAR(50) NOT NULL,          -- Source Product ID
    product_category_name   VARCHAR(100),                  -- Product Category
    product_name_length     INT,
    product_description_length INT,
    product_weight_g        INT,
    -- Physical Dimension Attributes
    product_length_cm       INT, 
    product_height_cm       INT, 
    product_width_cm        INT, 
    DWH_create_date         DATETIME2 DEFAULT GETDATE()    -- Metadata: Record creation date
);
GO

-- Dim_Seller Definition (DDL)
IF OBJECT_ID('Gold.Dim_Seller', 'U') IS NOT NULL DROP TABLE Gold.Dim_Seller;
CREATE TABLE Gold.Dim_Seller (
    Seller_Key              INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    seller_id               VARCHAR(50) NOT NULL,          -- Source Seller ID
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(50),
    DWH_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- Fact_Sales Definition (Core Transactional Fact Table)
DROP TABLE IF EXISTS Gold.Fact_Sales; 
GO
CREATE TABLE Gold.Fact_Sales (
    -- Business Keys for Granularity
    Order_ID                VARCHAR(50)     NOT NULL,
    Order_Item_ID           INT             NOT NULL, 
    
    -- Dimension Surrogate Keys (Foreign Keys)
    Customer_Key            INT             NOT NULL DEFAULT 0,
    Product_Key             INT             NOT NULL DEFAULT 0,
    Seller_Key              INT             NOT NULL DEFAULT 0,
    
    -- Unified Time Dimension Keys (YYYYMMDD Format)
    Order_Purchase_Time_Key INT             NOT NULL DEFAULT 0,
    Order_Estimated_Time_Key INT            NOT NULL DEFAULT 0,
    Order_Delivered_Time_Key INT            NOT NULL DEFAULT 0,
    
    -- Fact Measures (Quantitative Data)
    Price                   DECIMAL(10, 2)  NOT NULL,      -- Item Unit Price
    Freight_Value           DECIMAL(10, 2)  NOT NULL       -- Shipping/Freight Cost
);
GO

-- Dim_Payment_Type Definition
IF OBJECT_ID('Gold.Dim_Payment_Type', 'U') IS NOT NULL DROP TABLE Gold.Dim_Payment_Type;
CREATE TABLE Gold.Dim_Payment_Type (
    Payment_Key             INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    payment_type            VARCHAR(50) NOT NULL,          -- Raw payment method
    payment_type_group      VARCHAR(50),                   -- Logical grouping (e.g., Digital vs Manual)
    DWH_create_date         DATETIME2 DEFAULT GETDATE()
);

-- Dim_Time Definition (Calendar Table)
IF OBJECT_ID('Gold.Dim_Time', 'U') IS NOT NULL DROP TABLE Gold.Dim_Time;
CREATE TABLE Gold.Dim_Time (
    Time_Key            INT PRIMARY KEY, -- Smart Key format (YYYYMMDD)
    Full_Date           DATE NOT NULL,
    Calendar_Year       INT NOT NULL,
    Calendar_Quarter    INT NOT NULL,
    Calendar_Month      INT NOT NULL,
    Day_of_Month        INT NOT NULL,
    Day_of_Week         VARCHAR(10) NOT NULL, 
    Week_of_Year        INT NOT NULL,
    Is_Weekend          BIT DEFAULT 0,
    DWH_create_date     DATETIME2 DEFAULT GETDATE()
);

-- Fact_Order_Reviews Definition
IF OBJECT_ID('Gold.Fact_Order_Reviews', 'U') IS NOT NULL DROP TABLE Gold.Fact_Order_Reviews;
CREATE TABLE Gold.Fact_Order_Reviews (
    -- Business Keys
    Review_ID                   VARCHAR(250)     NOT NULL,
    Order_ID                    VARCHAR(250)     NOT NULL,
    
    -- Surrogate Dimension Keys
    Customer_Key                INT             NOT NULL DEFAULT 0,
    Product_Key                 INT             NOT NULL DEFAULT 0,
    Review_Creation_Time_Key    INT             NOT NULL DEFAULT 0,
    
    -- Quantitative Review Measures
    Review_Score                INT             NOT NULL, -- Numeric Rating (1-5)
    Review_Comment_Length       INT             NULL      -- Length of text feedback
);
GO

-- Dim_Geolocation Definition (Spatial Data)
DROP TABLE IF EXISTS Gold.Dim_Geolocation;
GO
CREATE TABLE Gold.Dim_Geolocation (
    Geolocation_Key         INT IDENTITY(1,1) PRIMARY KEY,
    geolocation_zip_code_prefix INT             NOT NULL, 
    geolocation_city        VARCHAR(150)    NOT NULL, 
    geolocation_state       CHAR(20)         NOT NULL 
);

-- Fact_Geolocation Definition (Logistic & Distance Analysis)
DROP TABLE IF EXISTS Gold.Fact_Geolocation;
CREATE TABLE Gold.Fact_Geolocation (
    Order_ID                    VARCHAR(50)     NOT NULL,
    Customer_Geolocation_Key    INT             NOT NULL DEFAULT 0, -- FK to Geolocation (Customer)
    Seller_Geolocation_Key      INT             NOT NULL DEFAULT 0, -- FK to Geolocation (Seller)
    Order_Purchase_Time_Key     INT             NOT NULL DEFAULT 0,
    Order_Delivered_Time_Key    INT             NOT NULL DEFAULT 0,
    Total_Freight_Value         DECIMAL(10, 2)  NOT NULL, 
    Delivery_Time_Days          INT             NULL      -- Metric: Actual duration of delivery
);
GO

-- Data Quality Cleanup: Handling NULLs in Time Keys
-- Replacing '0' placeholders with NULL for proper referential integrity and Power BI analysis
UPDATE gold.Fact_Sales SET Order_Delivered_Time_Key = NULL WHERE Order_Delivered_Time_Key = 0;
UPDATE gold.Fact_Sales SET Order_Estimated_Time_Key = NULL WHERE Order_Estimated_Time_Key = 0;

UPDATE gold.Fact_Order_Header SET Approved_Time_Key = NULL WHERE Approved_Time_Key = 0;
UPDATE gold.Fact_Order_Header SET Purchase_Time_Key = NULL WHERE Purchase_Time_Key = 0;