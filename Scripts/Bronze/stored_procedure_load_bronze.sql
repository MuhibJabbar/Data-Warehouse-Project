/*
This stored procedure bronze.load_bronze is a bronze-layer batch loader that refreshes your raw tables from CSV files and 
logs how long each load takes. When you run it, it captures a batch start time, then for each bronze table 
(CRM customer, product, sales; ERP customer, location, category) it records a table start time, TRUNCATEs the target table 
to remove any previous rows (so re-running won’t create duplicates), and then uses BULK INSERT to load the CSV from a 
local path, skipping the header row (FIRSTROW = 2), reading comma-separated columns (FIELDTERMINATOR = ','), 
and using TABLOCK for faster bulk loading. After each table load it prints the duration in seconds and a separator line, 
and at the end it prints the total duration for loading the entire bronze layer batch.
*/

Create or Alter procedure bronze.load_bronze as
Begin
	Declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime

	Set @batch_start_time = GETDATE(); -- starting time of whole bronze batch

	Set @start_time = GETDATE(); -- starting time of loading table
	TRUNCATE TABLE bronze.crm_cust_info; -- turnicating table if already existing
	Bulk Insert bronze.crm_cust_info -- inserting table
	From 'C:\Users\muhib\OneDrive\Desktop\Data warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	With (
		firstrow = 2, -- starting from 2nd row
		fieldterminator = ',', -- in our file columns are seprated by comma ,
		Tablock -- for bulk insert we use this for improve the performance
	);
	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';

	Set @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_prd_info;
	Bulk Insert bronze.crm_prd_info 
	From 'C:\Users\muhib\OneDrive\Desktop\Data warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
	);
	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';

	Set @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_sales_details;
	Bulk Insert bronze.crm_sales_details 
	From 'C:\Users\muhib\OneDrive\Desktop\Data warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
	);
	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';

	Set @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_cust_az12;
	Bulk Insert bronze.erp_cust_az12 
	From 'C:\Users\muhib\OneDrive\Desktop\Data warehouse project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
	);
	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';

	Set @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_loc_a101;
	Bulk Insert bronze.erp_loc_a101 
	From 'C:\Users\muhib\OneDrive\Desktop\Data warehouse project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
	);
	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';

	Set @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	Bulk Insert bronze.erp_px_cat_g1v2
	From 'C:\Users\muhib\OneDrive\Desktop\Data warehouse project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	With (
		firstrow = 2,
		fieldterminator = ',',
		Tablock
	);
	Set @end_time = GETDATE();
	print 'Loading Duration:' + cast(Datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
	print '-----------------------------------------------------------------------------------';

	Set @batch_end_time= GETDATE();
	print''
	print'Loading duration of whole bronze layer: ' + 
	cast(Datediff(second, @batch_start_time,@batch_end_time) as nvarchar) + ' seconds';

End

