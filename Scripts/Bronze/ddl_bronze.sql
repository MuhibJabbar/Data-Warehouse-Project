/*
DDL Script: Create Bronze Tables

This script is used to initialize the bronze (raw) layer tables of a data warehouse by ensuring a clean and 
consistent schema before data loading. For each table, it first checks whether the table already exists using 
OBJECT_ID(..., 'U'); if it does, the table is dropped to avoid schema conflicts or duplicate data. 
It then recreates the table with explicitly defined columns and data types that closely mirror the source systems 
(CRM and ERP), without applying transformations, constraints, or business rules. The tables cover customer, product, sales, 
and reference data and are intentionally simple and flexible, which aligns with bronze-layer best practices—store 
raw data as received so it can later be transformed, cleansed, and enriched in the silver and gold layers of the data 
warehouse.
*/

if OBJECT_ID('bronze.crm_cust_info','U') is not null 
	drop table bronze.crm_cust_info;
Create table bronze.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date
);
if OBJECT_ID('bronze.crm_prd_info','U') is not null 
	drop table bronze.crm_prd_info;
Create table bronze.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime
);
if OBJECT_ID('bronze.crm_sales_details','U') is not null 
	drop table bronze.crm_sales_details;
Create table bronze.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);
if OBJECT_ID('bronze.erp_cust_az12','U') is not null 
	drop table bronze.erp_cust_az12;
Create Table bronze.erp_cust_az12(
	CID nvarchar(50),
	BDATE date,
	GEN nvarchar(50)
);
if OBJECT_ID('bronze.erp_Loc_A101','U') is not null 
	drop table bronze.erp_Loc_A101;
Create Table bronze.erp_Loc_A101(
	CID nvarchar(50),
	CNTRY nvarchar(50)
);
if OBJECT_ID('bronze.erp_px_cat_g1v2','U') is not null 
	drop table bronze.erp_px_cat_g1v2;
Create Table bronze.erp_px_cat_g1v2(
	ID nvarchar(50),
	CAT	nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50)
);
