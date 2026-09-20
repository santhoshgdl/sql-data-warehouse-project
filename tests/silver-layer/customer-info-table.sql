-- Check for NULL's or duplicates in the Primary Key 
-- Expectation - No Result


SELECT * FROM 
bronze.crm_cust_info;

-- Step: 1 - we are checking the NULL's and non-unique id's in the table.
SELECT 
	cst_id,
	COUNT(*)
FROM 
	bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
ORDER BY COUNT(*) DESC;

/* 
	Step: 2 -- Observe the non-unique value
	-- Quality Check - A primary key must be unique and NOT NULL
	Here for this value we can use the latest create date.
	We are ranking the non-unique id's with the ROW_NUMBER() function and we are considering the highest rank row.
*/

SELECT 
	* 
FROM 
(
	SELECT 
		* ,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM 
		bronze.crm_cust_info 
	WHERE cst_id = 29466
) t
WHERE flag_last = 1;


/*
	Step: 3 - Quality Check - Check for Unwanted spaces in the string values.
	Expectation - No Results
	Here we are using the TRIM() Function 
		If the original value is not equal to the same value after trimming,
		it means there are spaces.

	SELECT 
		cst_firstname 
	FROM 
		bronze.crm_cust_info 
	WHERE cst_firstname != TRIM(cst_firstname);

*/

SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date
FROM 
(
	SELECT 
		* ,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM 
		bronze.crm_cust_info 
	WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;

/*
	Step: 4 - Quality Check - Check the consistency of values in low cardinality columns.
	In our data warehouse,
	we aim to store clear and meaningful values rather than using abbreviated terms.
	and use 'N/A' for the missing values.
	Remove any unwanted spaces using TRIM() Function.

	SELECT 
		DISTINCT cst_gndr 
	FROM 
		bronze.crm_cust_info;
*/

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date
)
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	ELSE 'N/A'
	END AS cst_material_status,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'N/A'
	END AS cst_gndr,
	cst_create_date
FROM 
(
	SELECT 
		* ,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM 
		bronze.crm_cust_info 
	WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;
