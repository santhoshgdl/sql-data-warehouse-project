/*
  DDL Script : CREATE Bronze Tables
  Script Purpose: 
    This script creates tables in the bronze schema, dropping existing tables,
    if they already exist.
    Run this script to re-define the DDL Structure of bronze tables.
*/
CREATE DATABASE Datawarehouse;

USE Datawarehouse;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO -- seperate batches when working with muliple SQL Statements.
CREATE SCHEMA gold;
GO

/*
	BRONZE LAYER:
		1. Analysing - Interview source system experts
		2. Coding - Data Ingestion
		3. Validating - Data Completeness & Schema checks.
		4. Docs & Version - Data documenting, versioning in GIT

	1. Analysing.

		a. Business context & ownership
			- who owns the data?
				like who is responsible for the data and which IT departments and so on.
			- what business process it supports?
				like supporting customer transactions,the supplychain logistics or maybe
				finance reporting.
			- system and data documentation.
			- data model and data catalog.

		b. Architecture & technology stack.
			- how is data stored ?
				SQL server, AWS, Azure, Oracle...
			- what are integration capabilities ?
				API, kafka, File Extract, Direct DB.

		c. Extract & load
			- Incremental vs full loads.
			- Data scope & Historical needs.
			- what is the expected size of the extracts.
			- are there any data volume limitations.
			- how to avoid impacting the source system's performance?
			- authentication and authorization. 
				tokens, SSH keys, VPN, IP Whitelisting...

		Create DDL for tables.
			Data definition language defines the structure of the datatable tables.
			consult the technical experts of the source system to understand its metadata.

		Data profiling 
			explore the data to identify column names and data types.


		
*/

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(10)
);


IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);

