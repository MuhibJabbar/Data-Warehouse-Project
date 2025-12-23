/*
This script will help you to create DataWarehouse Database. It the database already exists iet will help ypu to drop the
old script and create the new one.

This script will also help you to create 3 schemas for the DataWarehouse Project.
1. Bronze Schema
2. Silver Schema
3. Gold Schema

*/

-- Drop already existing DataWarehouse database

if exists (select 1 from sys.databases where name = 'DataWarehouse')
Begin
	alter Database DataWarehouse SET Single_User With Rollback Immediate;
	Drop Database DataWarehouse;
End;

-- Create and Use the 'DateWarehouse' Database

Create Database DataWarehouse;
Go

Use DataWarehouse;
Go

--Create Schema's

Create Schema Bronze;
Go
Create Schema Silver;
Go
Create Schema Gold;
Go
