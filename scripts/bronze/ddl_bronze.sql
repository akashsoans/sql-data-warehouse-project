/*
=====================================================================================================
DDL Scripts: Create bronze tables
=====================================================================================================
Script Purpose:
This script creates tables in the 'bronze' schema, dropping existing tables if they already exists.

Run this script to re-define the DDL structure of 'bronze' tables.
=====================================================================================================
*/

DROP TABLE IF EXISTS bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info (
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_material_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
)

DROP TABLE IF EXISTS bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info (
prd_id	INT,
prd_key	VARCHAR(50),
prd_nm	VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
)

DROP TABLE IF EXISTS bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details (
sls_ord_num	VARCHAR(50),
sls_prd_key	VARCHAR(50),
sls_cust_id	INT,
sls_order_dt INT,	
sls_ship_dt	INT,
sls_due_dt INT,	
sls_sales INT,	
sls_quantity INT,	
sls_price INT
)

DROP TABLE IF EXISTS bronze.erp_CUST_AZ12
CREATE TABLE bronze.erp_CUST_AZ12(
CID	VARCHAR(50),
BDATE DATE,
GEN VARCHAR(50)
)

DROP TABLE IF EXISTS bronze.erp_LOC_A101
CREATE TABLE bronze.erp_LOC_A101(
CID VARCHAR(50),
CNTRY VARCHAR(50)
)

DROP TABLE IF EXISTS bronze.erp_PX_CAT_G1V2
CREATE TABLE bronze.erp_PX_CAT_G1V2(
ID	VARCHAR(50),
CAT	VARCHAR(50),
SUBCAT	VARCHAR(50),
MAINTENANCE VARCHAR(50)
)

TRUNCATE table bronze.crm_cust_info;
COPY bronze.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_material_status,cst_gndr ,cst_create_date)
FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'DELIMITER ',' CSV HEADER;

TRUNCATE table bronze.crm_prd_info;
COPY bronze.crm_prd_info(prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'DELIMITER ',' CSV HEADER;

TRUNCATE table bronze.crm_sales_details;
COPY bronze.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,	sls_sales,sls_quantity,	sls_price)
FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'DELIMITER ',' CSV HEADER;

TRUNCATE table bronze.erp_CUST_AZ12;
COPY bronze.erp_CUST_AZ12(CID,BDATE,GEN)
FROM 'C:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'DELIMITER ',' CSV HEADER;

TRUNCATE table bronze.erp_LOC_A101;
COPY bronze.erp_LOC_A101(CID,CNTRY)
FROM 'C:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'DELIMITER ',' CSV HEADER;

TRUNCATE table  bronze.erp_PX_CAT_G1V2;
COPY bronze.erp_PX_CAT_G1V2(ID,CAT,SUBCAT,MAINTENANCE)
FROM 'C:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'DELIMITER ',' CSV HEADER;


select * from bronze.crm_cust_info;
select * from bronze.crm_prd_info;
select * from bronze.crm_sales_details;
select * from bronze.erp_CUST_AZ12;
select * from bronze.erp_LOC_A101;
select * from bronze.erp_PX_CAT_G1V2;

