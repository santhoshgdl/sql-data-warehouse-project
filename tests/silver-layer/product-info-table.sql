/*

	Step-1: 
		Checking duplicates and NULL's in the prd_id 
		Result - No Duplicates.

	Step-2: 
		1.Creating new column 'cat_id' to join with the erp_px_cat_g1v2 table in the gold layer
		2.Making it matching with the cat_id in the erp_px_cat_g1v2 table by using REPLACE() and SUBSTRING() Functions 
		3.Filters out unmatched data after applying transformation.

*/

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	prd_nm,
	prd_cost, 
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM 
	bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
(
	SELECT 
		DISTINCT id 
	FROM 
		bronze.erp_px_cat_g1v2
);

/*
	Step: 3 - Creatinig the prd_key to join with the sales_details table in the gold layer.
*/

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key,
	prd_nm,
	prd_cost, 
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM 
	bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, len(prd_key)) IN 
(
	SELECT 
		sls_prd_key
	FROM 
		bronze.crm_sales_details
);


/*
	Step: 4 - Check for NULLs or Negative numbers.
	USE COALESCE() or ISNULL() Functions to replace null values with a specific replacement value.

	SELECT 
		prd_id,
		prd_cost
	FROM 
		bronze.crm_prd_info 
	WHERE prd_cost < 0 OR prd_cost IS NULL;
*/

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost, 
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM 
	bronze.crm_prd_info;

/*
	Step: 5 - Data Standardisation 

	SELECT 
		DISTINCT prd_line 
	FROM bronze.crm_prd_info;
*/
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost, 
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
	ELSE 'N/A'
	END AS prd_line,
	prd_start_dt,
	prd_end_dt
FROM 
	bronze.crm_prd_info;

/*
	Step: 6 - Check for Invalid Order Dates.
	Each Record must have a start date - NO NULLs

	SELECT 
		* 
	FROM 
		bronze.crm_prd_info 
	WHERE 
		prd_end_dt < prd_start_dt;


	Here we are using the only start date and end_date will be the 
	next value of the start_date less than 1 day.
	USING LEAD() Function.

	SELECT 
		prd_id,
		prd_key,
		prd_nm,
		prd_start_dt,
		prd_end_dt,
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
	FROM  
		bronze.crm_prd_info
	WHERE 
		prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

	In these columns, time is 0, so we can convert to DATE datatype
*/

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost, 
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
	ELSE 'N/A'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM 
	bronze.crm_prd_info;

/*
	Step: 7 - Create new product info table 
*/

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME DEFAULT GETDATE()
);	

/*
	Step: 8 - Insert the data into silver layer table
*/

INSERT INTO silver.crm_prd_info
(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    dwh_create_date
)
SELECT 
    prd_id,

    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,

    prd_nm,

    ISNULL(prd_cost, 0) AS prd_cost, 

    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'N/A'
    END AS prd_line,

    CAST(prd_start_dt AS DATE) AS prd_start_dt,

    CAST(
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key 
            ORDER BY prd_start_dt
        ) - 1 
        AS DATE
    ) AS prd_end_dt,

    GETDATE() AS dwh_create_date

FROM bronze.crm_prd_info;
