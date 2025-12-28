🏗️ Olist E-Commerce End-to-End Data Pipeline & Analytics
🌟 Project Overview

This project is a comprehensive Data Production Pipeline designed to transform raw e-commerce data from the Olist Dataset into actionable business insights. It demonstrates a full ETL (Extract, Transform, Load) process, transitioning data from a Silver staging layer to a highly optimized Gold layer using a Star Schema architecture.
🛠️ Technical Architecture

The project is built on a modern data stack to ensure scalability and performance:

    Data Warehouse Structure: Implemented a multi-layer architecture (Silver to Gold).

    Data Modeling: Developed a robust Star Schema consisting of high-performance Fact tables (Sales, Geolocation, Reviews) and descriptive Dimension tables (Customer, Product, Seller, Time).

    Automation: Engineered automated SQL Stored Procedures to handle data refresh, cleaning, and complex transformations.

    Data Quality: Implemented advanced cleaning techniques, including Statistical Imputation (Mean/Average) for missing product metrics and Proxy Logic for geolocation data gaps.

📊 Business Insights & Analytics (Power BI)

The final production layer is visualized through an interactive Executive Dashboard that tracks key performance indicators (KPIs):

    Financial Metrics: Real-time tracking of Total Revenue ($14M) and Average Order Value ($137.75).

    Sales Trends: Monthly sales analysis using Area Charts to identify seasonal trends and growth patterns.

    Operational Intelligence: Logistics tracking and top-performing product categories (e.g., Health & Beauty, Watch Gifts).

    Geospatial Analysis: Mapping sales distribution across different states to optimize delivery and marketing efforts.

🔑 Key Features

    Dynamic Time Dimension: A custom-coded calendar generator (2016-2018) to enable advanced Time Intelligence (YoY, MTD growth).

    Optimized Performance: Used Integer Surrogate Keys (YYYYMMDD) for the time dimension to ensure lightning-fast report performance.

    User-Centric Design: Focused on a clean, professional UI with high-contrast elements for better readability.
