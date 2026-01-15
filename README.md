# 🛒 Olist E-commerce: From 1.5M Raw Records to $14M in Actionable Insights

![SQL Server](https://img.shields.io/badge/SQL_Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Data Engineering](https://img.shields.io/badge/Architecture-Medallion-blue?style=for-the-badge)

## 🚀 Project Overview
This isn't just a dashboard; it's a **Production-Grade Data Pipeline**. I built a robust, scalable architecture to process over **1.58 Million records** from the Olist E-commerce dataset, transforming raw, 9 heterogeneous CSV files data into a high-performance Data Warehouse.

The core challenge was **Performance Engineering at Scale**. Without big data frameworks, I pushed SQL Server to its limits by optimizing Stored Procedures and Joins to ensure maximum execution efficiency.

---

## 🏗️ Technical Architecture: The Medallion Approach

The project follows the **Medallion Architecture**, ensuring data integrity and quality at every stage.

### 1️⃣ Data Pipeline Layers (Bronze to Gold)
I implemented a structured pipeline to manage data evolution, starting from raw ingestion to analytical readiness.
* **Bronze Layer:** Raw ingestion of 9 heterogeneous CSV sources.
* **Silver Layer:** Automated cleansing and standardization.
* **Gold Layer:** Optimized tables for business intelligence.

<p align="center">
  <img src="https://github.com/YoussefHamedddd/End-to-End-ECommerce-Warehouse-Analytics/blob/main/Docs/Tables.jpeg?raw=true" width="400" alt="Tables">
</p>

### 2️⃣ Data Modeling (Galaxy Schema)
The architecture connects multiple Fact tables with specialized Dimensions to allow for complex analysis using an optimized **Star Schema**.

<p align="center">
  <img src="https://github.com/YoussefHamedddd/End-to-End-ECommerce-Warehouse-Analytics/blob/main/Docs/Star%20Schema.jpeg?raw=true" width="800" alt="Star Schema Architecture">
</p>

---

## 📊 Executive Dashboard & KPIs
Designed an interactive Power BI suite tracking **$14M in revenue** and **99K orders**.

### 📈 Overview & Sales Performance
Focuses on financial health, tracking revenue trends, and top-selling categories.
* **Total Revenue:** $14M
* **Total Orders:** 99K
* **Avg. Order Value:** $137.75
<p align="center">
  <img src="https://github.com/YoussefHamedddd/End-to-End-ECommerce-Warehouse-Analytics/blob/main/Pages%20of%20Dashboard/OverView.jpg" width="900" alt="Executive Overview">
</p>

### 🚚 Logistics & Operations
Monitoring shipping efficiency and order status distribution to ensure operational excellence.
* **Avg. Delivery Time:** 0.52 Days.
* **Freight % of Revenue:** 16.57%.
<p align="center">
  <img src="https://github.com/YoussefHamedddd/End-to-End-ECommerce-Warehouse-Analytics/blob/main/Pages%20of%20Dashboard/Logistics%20%26%20Operations%20.jpg" width="900" alt="Logistics Dashboard">
</p>

### 🛍️ Product Insights & Customer Loyalty
Analyzing customer feedback and identifying loyal customer segments.
* **Avg. Review Score:** 4.09 / 5.0.
<p align="center">
  <img src="https://github.com/YoussefHamedddd/End-to-End-ECommerce-Warehouse-Analytics/blob/main/Pages%20of%20Dashboard/Product%20Insights%20.jpg" width="900" alt="Product Insights">
</p>

### 👤 Seller Performance Overview
Deep dive into seller activity, prep times, and geographic concentration.
* **Active Sellers:** 3,095.
<p align="center">
  <img src="https://github.com/YoussefHamedddd/End-to-End-ECommerce-Warehouse-Analytics/blob/main/Pages%20of%20Dashboard/Sellers%20Insights%20.jpg" width="900" alt="Seller Performance">
</p>

---

## 🛠️ Performance Engineering (SQL Sample)
I focused heavily on ensuring the Gold layer was query-optimized. Below is a sample of the validation queries used to test the business logic and performance.

<p align="center">
  <img src="https://github.com/YoussefHamedddd/End-to-End-ECommerce-Warehouse-Analytics/blob/main/Docs/Sample.jpeg" width="700" alt="SQL Code Sample">
</p>

```sql
-- Analyzing Sales Performance & Monthly Revenue
SELECT 
    T.Calendar_Year,
    T.Calendar_Month,
    SUM(S.price) AS Monthly_Revenue,
    COUNT(S.Order_ID) AS Total_Orders
FROM Gold.Fact_Sales S
JOIN Gold.Dim_Time T ON S.Order_Purchase_Time_Key = T.Time_Key
GROUP BY T.Calendar_Year, T.Calendar_Month;
