/*******************************************************************************
    PROJECT: E-Commerce Data Warehouse (Gold Layer)
    PURPOSE: Business Analytics & Performance KPI Testing
*******************************************************************************/

-- 1. Sales Performance: What is the total revenue and order count per month?
SELECT 
    T.Calendar_Year, 
    T.Calendar_Month, 
    SUM(S.price) AS Monthly_Revenue,
    COUNT(S.Order_ID) AS Total_Orders
FROM Gold.Fact_Sales S
JOIN Gold.Dim_Time T ON S.Order_Purchase_Time_Key = T.Time_Key
GROUP BY T.Calendar_Year, T.Calendar_Month
ORDER BY T.Calendar_Year, T.Calendar_Month;


-- 2. Customer Geography: Which states have the highest number of customers?
SELECT 
    G.geolocation_state, 
    COUNT(C.Customer_Key) AS Customer_Count
FROM Gold.Dim_Customer C
JOIN Gold.Dim_Geolocation G ON C.Geolocation_Key = G.Geolocation_Key
GROUP BY G.geolocation_state
ORDER BY Customer_Count DESC;


-- 3. Product Analytics: What are the top 5 product categories by revenue?
SELECT TOP 5 
    P.product_category_name, 
    SUM(S.price) AS Category_Revenue
FROM Gold.Fact_Sales S
JOIN Gold.Dim_Product P ON S.Product_Key = P.Product_Key
GROUP BY P.product_category_name
ORDER BY Category_Revenue DESC;


-- 4. Payment Methods: What is the preferred payment type for our customers?
SELECT 
    PT.payment_type, 
    COUNT(S.Order_ID) AS Usage_Count,
    SUM(S.price) AS Total_Paid
FROM Gold.Fact_Sales S
JOIN Gold.Dim_Payment_Type PT ON S.Payment_Key = PT.Payment_Key
GROUP BY PT.payment_type
ORDER BY Usage_Count DESC;


-- 5. Delivery Performance: What is the average shipping cost per state?
SELECT 
    G.geolocation_state, 
    AVG(S.freight_value) AS Average_Shipping_Cost
FROM Gold.Fact_Sales S
JOIN Gold.Dim_Customer C ON S.Customer_Key = C.Customer_Key
JOIN Gold.Dim_Geolocation G ON C.Geolocation_Key = G.Geolocation_Key
GROUP BY G.geolocation_state
ORDER BY Average_Shipping_Cost DESC;


-- 6. Customer Satisfaction: Which products have the lowest review scores?
-- Note: Requires joining Fact_Order_Reviews
SELECT TOP 10 
    P.product_id, 
    AVG(R.Review_Score) AS Avg_Rating
FROM Gold.Fact_Order_Reviews R
JOIN Gold.Dim_Product P ON R.Product_Key = P.Product_Key
GROUP BY P.product_id
HAVING COUNT(R.Review_ID) > 5 -- Only products with more than 5 reviews
ORDER BY Avg_Rating ASC;




/*******************************************************************************
    ADVANCED ANALYTICS: E-Commerce Growth & Operations
*******************************************************************************/

-- 1. Customer Loyalty: How many customers have placed more than one order? (Retention)
SELECT 
    CASE WHEN Order_Count > 1 THEN 'Repeat Customer' ELSE 'One-time Buyer' END AS Customer_Type,
    COUNT(*) AS Total_Customers
FROM (
    SELECT customer_unique_id, COUNT(Customer_Key) AS Order_Count
    FROM Gold.Dim_Customer
    GROUP BY customer_unique_id
) AS CustomerStats
GROUP BY CASE WHEN Order_Count > 1 THEN 'Repeat Customer' ELSE 'One-time Buyer' END;




-- 2. Revenue Concentration: What percentage of total revenue comes from the Top 10% of products?
-- (Pareto Principle / 80-20 Rule)
WITH ProductRevenue AS (
    SELECT Product_Key, SUM(price) AS Revenue,
           PERCENT_RANK() OVER (ORDER BY SUM(price) DESC) AS RevenueRank
    FROM Gold.Fact_Sales
    GROUP BY Product_Key
)
SELECT 
    SUM(CASE WHEN RevenueRank <= 0.1 THEN Revenue ELSE 0 END) AS Revenue_From_Top_10_Percent,
    SUM(Revenue) AS Total_Global_Revenue,
    (SUM(CASE WHEN RevenueRank <= 0.1 THEN Revenue ELSE 0 END) / SUM(Revenue)) * 100 AS Percentage_Contribution
FROM ProductRevenue;


-- 3. Payment Risk: Which payment types have the highest number of "Canceled" orders?
SELECT 
    PT.payment_type,
    COUNT(H.Order_ID) AS Canceled_Orders
FROM Gold.Fact_Order_Header H
JOIN Gold.Fact_Sales S ON H.Order_ID = S.Order_ID
JOIN Gold.Dim_Payment_Type PT ON S.Payment_Key = PT.Payment_Key
WHERE H.Order_Status = 'canceled'
GROUP BY PT.payment_type
ORDER BY Canceled_Orders DESC;


-- 4. Sales Seasonality: What is the peak hour of the day for shopping?
SELECT 
    DATEPART(HOUR, H.order_purchase_timestamp) AS Hour_Of_Day,
    COUNT(H.Order_ID) AS Total_Orders
FROM Gold.Fact_Order_Header H
GROUP BY DATEPART(HOUR, H.order_purchase_timestamp)
ORDER BY Total_Orders DESC;





/*******************************************************************************
    PRO-LEVEL ANALYTICS: Product Affinity & Revenue Dynamics
*******************************************************************************/

-- 1. Market Basket Analysis: Which products are frequently bought together?
-- This helps in designing "Frequently Bought Together" bundles.
SELECT 
    p1.product_category_name AS Product_A, 
    p2.product_category_name AS Product_B, 
    COUNT(*) AS Times_Bought_Together
FROM Gold.Fact_Sales s1
JOIN Gold.Fact_Sales s2 ON s1.Order_ID = s2.Order_ID AND s1.Product_Key < s2.Product_Key
JOIN Gold.Dim_Product p1 ON s1.Product_Key = p1.Product_Key
JOIN Gold.Dim_Product p2 ON s2.Product_Key = p2.Product_Key
GROUP BY p1.product_category_name, p2.product_category_name
ORDER BY Times_Bought_Together DESC;


-- 2. Seller Quality: Which sellers have the highest revenue but the lowest review scores?
-- This identifies high-volume sellers that might be harming the brand reputation.
SELECT TOP 10
    S.seller_id,
    SUM(Sales.price) AS Total_Revenue,
    AVG(R.Review_Score) AS Avg_Seller_Rating
FROM Gold.Fact_Sales Sales
JOIN Gold.Dim_Seller S ON Sales.Seller_Key = S.Seller_Key
LEFT JOIN Gold.Fact_Order_Reviews R ON Sales.Product_Key = R.Product_Key -- Assuming review link
GROUP BY S.seller_id
HAVING AVG(R.Review_Score) < 3
ORDER BY Total_Revenue DESC;


-- 3. Revenue Growth (MoM): What is the Month-over-Month percentage growth in revenue?
-- This is a key KPI for any E-commerce business.
WITH MonthlySales AS (
    SELECT 
        T.Calendar_Year, T.Calendar_Month,
        SUM(S.price) AS Current_Month_Revenue
    FROM Gold.Fact_Sales S
    JOIN Gold.Dim_Time T ON S.Order_Purchase_Time_Key = T.Time_Key
    GROUP BY T.Calendar_Year, T.Calendar_Month
)
SELECT 
    Calendar_Year, Calendar_Month, Current_Month_Revenue,
    LAG(Current_Month_Revenue) OVER (ORDER BY Calendar_Year, Calendar_Month) AS Previous_Month_Revenue,
    ((Current_Month_Revenue - LAG(Current_Month_Revenue) OVER (ORDER BY Calendar_Year, Calendar_Month)) 
    / NULLIF(LAG(Current_Month_Revenue) OVER (ORDER BY Calendar_Year, Calendar_Month), 0)) * 100 AS Growth_Percentage
FROM MonthlySales;


-- 4. Customer Concentration: Who are the "Whales" (Top 1% of customers by spending)?
-- These are the VIPs for marketing loyalty programs.
SELECT TOP 1 PERCENT 
    C.customer_unique_id,
    SUM(S.price) AS Lifetime_Value
FROM Gold.Fact_Sales S
JOIN Gold.Dim_Customer C ON S.Customer_Key = C.Customer_Key
GROUP BY C.customer_unique_id
ORDER BY Lifetime_Value DESC;


-- 5. Dead Inventory: Which products have not been sold in the last 6 months?
-- This helps in identifying stock that should be discounted.
SELECT P.product_id, P.product_category_name
FROM Gold.Dim_Product P
WHERE P.Product_Key NOT IN (
    SELECT S.Product_Key 
    FROM Gold.Fact_Sales S
    JOIN Gold.Dim_Time T ON S.Order_Purchase_Time_Key = T.Time_Key
    WHERE T.Full_Date >= DATEADD(month, -6, GETDATE())
);






/*******************************************************************************
    STRATEGIC ANALYTICS: Customer Trends & Operational Efficiency
*******************************************************************************/

-- 1. Average Ticket Size (ATS): What is the average amount spent per order?
-- Helps understand customer spending power.
SELECT 
    AVG(Order_Total) AS Avg_Ticket_Size
FROM (
    SELECT Order_ID, SUM(price) AS Order_Total
    FROM Gold.Fact_Sales
    GROUP BY Order_ID
) AS Sub;



-- 2. Top Revenue Cities: Which cities generate the most sales?
SELECT TOP 10
    G.geolocation_city,
    SUM(S.price) AS Total_Sales
FROM Gold.Fact_Sales S
JOIN Gold.Dim_Customer C ON S.Customer_Key = C.Customer_Key
JOIN Gold.Dim_Geolocation G ON C.Geolocation_Key = G.Geolocation_Key
GROUP BY G.geolocation_city
ORDER BY Total_Sales DESC;






/*******************************************************************************
    PROFESSIONAL ANALYTICS: Profitability & Market Dynamics
*******************************************************************************/

-- 1. Freight Ratio: What is the percentage of shipping cost relative to product price?
-- High shipping costs can lead to canceled orders.
SELECT 
    product_category_name,
    AVG(freight_value) AS Avg_Shipping,
    AVG(price) AS Avg_Price,
    (AVG(freight_value) / NULLIF(AVG(price), 0)) * 100 AS Freight_Percentage
FROM Gold.Fact_Sales S
JOIN Gold.Dim_Product P ON S.Product_Key = P.Product_Key
GROUP BY product_category_name
ORDER BY Freight_Percentage DESC;



-- 2. Weekend vs Weekday Sales: Do customers buy more on weekends?
SELECT 
    CASE 
        WHEN T.Day_of_Week IN ('Saturday', 'Sunday') THEN 'Weekend' 
        ELSE 'Weekday' 
    END AS Day_Type,
    COUNT(DISTINCT S.Order_ID) AS Total_Orders,
    SUM(S.price) AS Total_Revenue
FROM Gold.Fact_Sales S
JOIN Gold.Dim_Time T ON S.Order_Purchase_Time_Key = T.Time_Key
GROUP BY 
    CASE 
        WHEN T.Day_of_Week IN ('Saturday', 'Sunday') THEN 'Weekend' 
        ELSE 'Weekday' 
    END;




-- 3. Order Concentration: Which 20% of cities generate 80% of the revenue?
-- Applying the Pareto Principle to Geography.
WITH CitySales AS (
    SELECT 
        G.geolocation_city, 
        SUM(S.price) AS Revenue,
        SUM(SUM(S.price)) OVER() AS Total_Global_Revenue,
        CUME_DIST() OVER(ORDER BY SUM(S.price) DESC) AS Revenue_Rank
    FROM Gold.Fact_Sales S
    JOIN Gold.Dim_Customer C ON S.Customer_Key = C.Customer_Key
    JOIN Gold.Dim_Geolocation G ON C.Geolocation_Key = G.Geolocation_Key
    GROUP BY G.geolocation_city
)
SELECT * FROM CitySales WHERE Revenue_Rank <= 0.2;




