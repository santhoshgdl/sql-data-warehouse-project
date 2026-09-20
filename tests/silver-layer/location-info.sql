SELECT 
	* 
FROM 
	bronze.erp_loc_a101;

/*
	Step:1 - Matching the cid with the customer info table.
*/

SELECT 
	REPLACE(cid, '-', '') AS cid
FROM 
	bronze.erp_loc_a101
WHERE 
	REPLACE(cid, '-', '') NOT IN 
	(
		SELECT 
			cst_key 
		FROM 
			silver.crm_cust_info
	);

/*
	Step: 2 - Checking quality of country column		
	
	SELECT
		DISTINCT cntry 
	FROM 
		bronze.erp_loc_a101;
*/

SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		ELSE TRIM(cntry) 
	END AS cntry
FROM 
	bronze.erp_loc_a101;
	
/*
	Step: 3 - Insert the values into the Silver Layer.
*/

INSERT INTO 
silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		ELSE TRIM(cntry) 
	END AS cntry
FROM 
	bronze.erp_loc_a101;
