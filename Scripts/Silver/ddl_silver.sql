/*
📌 Silver Layer Table Creation (CRM & ERP)

This SQL script drops and recreates Silver-layer tables used in a data warehouse.
The Silver layer stores cleaned, structured, and integrated data coming from raw (Bronze) CRM and ERP sources.

🔹 What this script does

- Checks if a table already exists using OBJECT_ID
- Drops the table if it exists (to avoid conflicts)
- Recreates the table with standardized column names and data types
- Adds dwh_create_date to track when data is loaded into the warehouse

🔹 Tables Created

CRM tables
- silver.crm_cust_info → Customer master data
- silver.crm_prd_info → Product master data
- silver.crm_sales_details → Sales transaction details

ERP tables
- silver.erp_cust_az12 → Customer demographic data
- silver.erp_Loc_A101 → Customer location data
- silver.erp_px_cat_g1v2 → Product category and hierarchy data

🔹 Key Design Notes
- nvarchar used for flexibility with source system values
- Dates stored using date, datetime, or datetime2
- dwh_create_date defaults to GETDATE() for audit and lineage tracking
- GO statements separate execution batches in SQL Server

📦 Purpose:
These tables act as the foundation for transformations into the Gold layer, enabling analytics, 
reporting, and business insights.
*/

if OBJECT_ID('silver.crm_cust_info','U') is not null 
	drop table silver.crm_cust_info;
Go

Create table silver.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()
);
Go

if OBJECT_ID('silver.crm_prd_info','U') is not null 
	drop table silver.crm_prd_info;
Create table silver.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime,
	dwh_create_date datetime2 default getdate()
);
Go

if OBJECT_ID('silver.crm_sales_details','U') is not null 
	drop table silver.crm_sales_details;
Create table silver.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);
Go

if OBJECT_ID('silver.erp_cust_az12','U') is not null 
	drop table silver.erp_cust_az12;
Create Table silver.erp_cust_az12(
	CID nvarchar(50),
	BDATE date,
	GEN nvarchar(50),
	dwh_create_date datetime2 default getdate()
);
Go

if OBJECT_ID('silver.erp_Loc_A101','U') is not null 
	drop table silver.erp_Loc_A101;
Create Table silver.erp_Loc_A101(
	CID nvarchar(50),
	CNTRY nvarchar(50),
	dwh_create_date datetime2 default getdate()
);
Go

if OBJECT_ID('silver.erp_px_cat_g1v2','U') is not null 
	drop table silver.erp_px_cat_g1v2;
Create Table silver.erp_px_cat_g1v2(
	ID nvarchar(50),
	CAT	nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50),
	dwh_create_date datetime2 default getdate()
);
Go
