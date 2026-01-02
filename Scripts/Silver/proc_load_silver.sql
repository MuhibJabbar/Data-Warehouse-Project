/*
✅ silver.load_silver Stored Procedure (Silver Layer ETL)

This procedure loads data from Bronze → Silver by applying data cleansing + standardisation and 
Then, inserting into Silver tables.

What it does (high level)

Tracks execution time for each table load and the full batch

For each dataset:

- TRUNCATE the Silver table (fresh reload)
- Apply transformations (cleanup, mappings, dedup, fixes)
- INSERT cleaned results into Silver tables
- Uses PRINT statements for step-by-step logging
- Transformations applied (per table)

1) silver.crm_cust_info

- Trims first/last names
- Normalizes marital status: M/S → Married/Single, else Unknown
- Normalizes gender: M/F → Male/Female, else Unknown
- Removes duplicates using ROW_NUMBER() and keeps the first record per cst_id
- Ignores rows where cst_id is null

2) silver.crm_prd_info (schema updated inside procedure)

- Drops & recreates table to match transformed structure

Extracts:
- cat_id from first part of prd_key
- prd_key_2 from remaining part of prd_key
- Replaces null prd_cost with 0
- Maps product line codes (M/R/S/T) into descriptive names
- Converts start date to date
- Creates prd_end_dt using LEAD() (end date = next start date − 1)

3) silver.crm_sales_details (schema updated inside procedure)

- Converts int-based dates (YYYYMMDD) into real date, invalid values → NULL
- Fixes sales: if null/invalid or doesn’t match qty * abs(price), recalculates it
- Fixes price: if null/invalid, derives price = sales / quantity safely using NULLIF

4) silver.erp_cust_az12

- Removes NAS prefix from customer id if present
- Sets future birthdates to NULL
- Normalizes gender values (F/FEMALE, M/MALE, else N/A)

5) silver.erp_Loc_A101

- Removes hyphens from customer id
- Normalizes country codes/names into standard country labels (DE→Germany, US→USA, etc.)
- Unknown/missing → N/A

6) silver.erp_px_cat_g1v2

Direct copy from Bronze to Silver (no transformations)

Notes (for repo clarity)

- This is a full refresh load (TRUNCATE + INSERT), not incremental.
- Some tables are dropped/recreated inside the procedure because the Silver schema is adjusted based on transformations.
- Final print shows total runtime for the whole Silver load.

*/


Create or Alter Procedure silver.load_silver as 
Begin

	Declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime

	Set @batch_start_time = GETDATE(); -- starting time of whole Silver batch
	
	Set @start_time = GETDATE(); -- starting time of loading table
	Print 'Turnicating the Table Silver.crm_cust_info '
	Truncate Table Silver.crm_cust_info;
	--Transformation of Bronze table CRM Cust Info and Load into silver table CRM Cust Info
	Print 'Inserting Into the Table Silver.crm_cust_info'
	Insert INTO Silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case when upper(trim (cst_marital_status)) = 'M' then 'Married'
		 when upper(trim (cst_marital_status)) = 'S' then 'Single'
		 else 'unknown'
	End as cst_marital_status,
	case when upper(trim(cst_gndr)) = 'M' then 'Male'
		 when upper(trim (cst_gndr)) = 'F' then 'Female'
		 else 'Unknown'
	End as cst_gndr,
	cst_create_date
	from(
		Select *,
		row_number() over(partition by cst_id order by cst_create_date) as flag
		from Bronze.crm_cust_info
		where cst_id is not null
	)t
		where flag = 1;
	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';

	Set @start_time = GETDATE(); -- starting time of loading table
	Print 'Turnicating the Table Silver.crm_prd_info '
	Truncate Table Silver.crm_prd_info;
	--Transformation of Bronze table CRM PRD Info and Load into silver table CRM PRD Info
	-- Also Updating the schema of the silver table due to transformation

	if OBJECT_ID('silver.crm_prd_info','U') is not null 
		drop table Silver.crm_prd_info;
	Create table silver.crm_prd_info(
		prd_id int,
		prd_key nvarchar(50),
		cat_id nvarchar(50),
		prd_key_2 nvarchar(50),
		prd_nm nvarchar(50),
		prd_cost int,
		prd_line nvarchar(50),
		prd_start_dt Date,
		prd_end_dt Date,
		dwh_create_date datetime2 default getdate()
	);

	Print 'Inserting Into the Table Silver.crm_prd_info'
	Insert INTO Silver.crm_prd_info(
		prd_id,
		prd_key,
		cat_id,
		prd_key_2,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)

		select 
			prd_id,
			prd_key,
			Replace(SUBSTRING(prd_key,1,5),'-','_') cat_id, -- Extract Category Id
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key_2, -- Extract 2nd Product Key
			prd_nm,
			isnull(prd_cost, 0) as prd_cost,
			Case when upper(trim(prd_line)) = 'M' then 'Mountain'
				 when upper(trim(prd_line)) = 'R' then 'Railway'
				 when upper(trim(prd_line)) = 'S' then 'Sea'
				 when upper(trim(prd_line)) = 'T' then 'Training'
				 Else 'N/A'
			End as prd_line, -- Map product line into descriptive value
			cast(prd_start_dt as date) prd_start_dt,
			cast(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) 
			as prd_end_dt -- Calculate end date as one day before the next date
		from Bronze.crm_prd_info;
	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';

	Set @start_time = GETDATE();
	Print 'Turnicating the Table Silver.crm_sales_details '
	Truncate Table Silver.crm_sales_details;
	--Transformation of Bronze Crm Sales Details and Load into silver table CRM Sales Details
	-- Also Updating the schema of the silver table due to transformation

	if OBJECT_ID('silver.crm_sales_details','U') is not null 
		drop table Silver.crm_sales_details;
	Create table silver.crm_sales_details(
		sls_ord_num nvarchar(50),
		sls_prd_key nvarchar(50),
		sls_cust_id int,
		sls_order_dt date,
		sls_ship_dt date,
		sls_due_dt date,
		sls_sales int,
		sls_quantity int,
		sls_price int,
		dwh_create_date datetime2 default getdate()
	);

	Print 'Inserting Into the Table Silver.crm_sales_details'
	Insert Into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)

		Select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			Case When sls_order_dt <= 0 or LEN(sls_order_dt) != 8 then Null -- handling invalid data 
				 else cast(cast(sls_order_dt as varchar)as date) -- casting data type 
			End as sls_order_dt,
			Case When sls_ship_dt <= 0 or LEN(sls_ship_dt) != 8 then Null
				 else cast(cast(sls_ship_dt as varchar)as date)
			End as sls_ship_dt,
			Case When sls_due_dt <= 0 or LEN(sls_due_dt) != 8 then Null
				 else cast(cast(sls_due_dt as varchar)as date)
			End as sls_due_dt,
			Case 
				 When sls_sales is null or sls_sales <=0 or 
				 sls_sales != sls_quantity * ABS(sls_price) -- handling null and invalid data 
				 then sls_quantity * ABS(sls_price)
				 else sls_sales
			End as sls_sales,
			sls_quantity,
			Case 
				when sls_price is null or sls_price <= 0 
				then sls_sales/nullif(sls_quantity,0)
				else sls_price
			End as sls_price
		from Bronze.crm_sales_details;

	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';


	set @start_time = GETDATE();
	Print 'Turnicating the Table Silver.erp_cust_az12 '
	Truncate Table Silver.erp_cust_az12;
	--Transformation of Bronze table ERP CUST AZ12 and Load into silver table ERP CUST AZ12
	Print 'Inserting Into the Table Silver.erp_cust_az12'
	Insert INTO Silver.erp_cust_az12 (
		CID,
		BDATE,
		GEN
	)

	Select 
	case when cid like 'NAS%' then substring(Cid,4,len(cid))
		 else CID
	end as CID, -- Remove NAS prefix if present
	case when BDATE > GETDATE() then Null
		 else BDATE
	end BDATE, -- Making future birthdate into NUll
	case 
		when Upper(trim(GEN)) IN ('F','FEMALE') then 'Female'
		when Upper(trim(GEN)) IN ('M','MALE') then 'Male'
		else 'N/A'
	end GEN -- handling null values and normalize gender values
	from Bronze.erp_cust_az12

	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';


	set @start_time = GETDATE();
	Print 'Turnicating the Table Silver.erp_Loc_A101 '
	Truncate Table Silver.erp_Loc_A101;
	--Transformation of Bronze table ERP LOC A101 and Load into silver table ERP LOC A101

	Print 'Inserting Into the Table Silver.erp_Loc_A101'
	Insert into Silver.erp_Loc_A101(
		CID,
		CNTRY
	)

	Select 
	REPLACE(CID,'-','') CID,
	Case When UPPER(TRIM(CNTRY)) IN ('DE','GERMANY') then 'Germany'
		 When UPPER(TRIM(CNTRY)) IN ('US','UNITED STATES') then 'USA'
		 When UPPER(TRIM(CNTRY)) IN ('AU','AUSTRALIA') then 'Australia'
		 When UPPER(TRIM(CNTRY)) IN ('FR','FRANCE') then 'France'
		 When UPPER(TRIM(CNTRY)) IN ('CA','CANADA') then 'Canada'
		 When UPPER(TRIM(CNTRY)) IN ('UK','UNITED KINGDOM') then 'United Kingdom'
		 Else 'N/A'
	End as Country -- Normalize and handle missing values and blank country codes
	from Bronze.erp_Loc_A101
	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';

	set @start_time = GETDATE();
	Print 'Turnicating the Table Silver.erp_px_cat_g1v2 '
	Truncate Table Silver.erp_px_cat_g1v2;
	--Transformation of Bronze table ERP PX CAT G1V2 and Load into silver table ERP PX CAT G1V2

	Print 'Inserting Into the Table Silver.erp_px_cat_g1v2'
	Insert Into Silver.erp_px_cat_g1v2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	)

	Select 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	from Bronze.erp_px_cat_g1v2

	Set @batch_end_time= GETDATE();
	print''
	print'Loading duration of whole silver layer: ' + 
	cast(Datediff(second, @batch_start_time,@batch_end_time) as nvarchar) + ' seconds';
End
