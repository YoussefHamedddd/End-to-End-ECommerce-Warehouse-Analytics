-- This script prepares the Data Warehouse environment.
-- 1. Switches to the master database.
-- 2. Creates a new database named 'ECommerce_DWH'.
-- 3. Switches context to the newly created database.
-- 4. Creates the three standard Medallion Architecture schemas:
--      - bronze  (raw data)
--      - silver  (cleaned and transformed data)
--      - gold    (business-ready data)


USE master;

CREATE DATABASE ECommerce_DWH 

USE ECommerce_DWH

GO
CREATE SCHEMA bronze
GO
CREATE SCHEMA silver 
GO
CREATE SCHEMA gold



