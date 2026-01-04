Gold Layer View Definitions (DDL)

This script drops and recreates Gold-layer views that expose analytics-ready datasets following a star schema design.

What this DDL does

- Safely drops existing Gold views using OBJECT_ID
- Recreates views with business-friendly column names
- Builds conformed dimensions and a fact table from Silver-layer data
- Uses GO to separate execution batches in SQL Server

Views Created
1️⃣ gold.dem_customers

Customer dimension view

- Combines CRM customer data with ERP demographic and location data
- Generates a surrogate customer key using ROW_NUMBER()
- CRM is treated as the master source for gender (ERP used as fallback)

2️⃣ gold.dem_products

Product dimension view

- Enriches CRM product data with ERP category hierarchy
- Filters to active products only (prd_end_dt IS NULL)
- Generates a surrogate product key

3️⃣ gold.fact_sales

Sales fact view

- Links sales transactions to customer and product dimensions
- Uses surrogate keys (customer_key, product_key) for joins
- Renames columns to business-friendly names for reporting

  ----------------------------------------------------------------------------------------------
  ----------------------------------------------------------------------------------------------

if OBJECT_ID('gold.dem_customers','V' is not null)
	Drop view gold.dem_customers;
GO

Create view gold.dem_customers As
	Select 
		ROW_NUMBER() over(order by cst_id) customer_key,
		ci.cst_id customer_id,
		ci.cst_key customer_number,
		ci.cst_firstname first_name,
		ci.cst_lastname last_name,
		cl.CNTRY country,
		ci.cst_marital_status marital_status,
		case when ci.cst_gndr != 'Unknown' then ci.cst_gndr -- CRM is the master table for gender
		else coalesce (ca.GEN,'N/A')
		end as gender,
		ci.cst_create_date create_date,
		ca.BDATE birth_date
	from Silver.crm_cust_info as ci
	left join Silver.erp_cust_az12 as ca
	on ci.cst_key = ca.CID
	left join Silver.erp_Loc_A101 cl
	on ci.cst_key = cl.CID

Go

if OBJECT_ID('gold.dem_products','V' is not null)
	Drop view gold.dem_products;
GO

create view gold.dem_products as
Select 
	row_number() over(order by prd_id) product_key,
	pn.prd_id product_id,
	pn.cat_id category_id,
	pn.prd_key_2 product_number,
	pn.prd_nm product_name,
	pn.prd_cost product_cost,
	pn.prd_line product_line,
	pn.prd_start_dt product_start_date,
	pc.CAT product_category,
	pc.SUBCAT product_sub_category,
	pc.MAINTENANCE 
from Silver.crm_prd_info pn
left join Silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.ID
where prd_end_dt is null -- only historical data which has no product end date

Go

if OBJECT_ID('gold.fact_sales','V' is not null)
	Drop view gold.fact_sales;
GO

Create view gold.fact_sales as
select 
	sls_ord_num order_number,
	pr.product_key product_key,
	cu.customer_key customer_key,
	sls_order_dt order_date,
	sls_ship_dt ship_date,
	sls_due_dt due_date,
	sls_sales sales_amount,
	sls_quantity quantity,
	sls_price price
from silver.crm_sales_details sd
left join Gold.dem_products pr
on sd.sls_prd_key = pr.product_number
left join Gold.dem_customers cu
on sd.sls_cust_id = cu.customer_id

Go

