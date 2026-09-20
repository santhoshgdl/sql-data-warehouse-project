/*
	Step-1: Matching with the cst_key in the crm_cust_info table to join.
	Checking whether we have other unmatched values.
*/

SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
		ELSE cid 
	END cid, 
bdate,
gen 
FROM 
	bronze.erp_cust_az12
WHERE 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
		ELSE cid 
	END NOT IN 
	(
		SELECT 
			DISTINCT cst_key 
		FROM
			silver.crm_cust_info
	);

/*
	Step: 2 - Check Out-of-range dates

	SELECT 
		DISTINCT bdate 
	FROM 
		bronze.erp_cust_az12
	WHERE 
		bdate < '1924-01-01' OR bdate > GETDATE();
*/

SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
		ELSE cid 
	END cid, 
	CASE	
		WHEN bdate > GETDATE() THEN NULL 
		ELSE bdate 
	END AS bdate,
	gen 
FROM 
	bronze.erp_cust_az12;

/*
	Step: 3 - Data Standardisation & Normalisation for gender column
	SELECT 
		DISTINCT gen
	FROM 
		bronze.erp_cust_az12;
*/


SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
		ELSE cid 
	END cid, 
	CASE	
		WHEN bdate > GETDATE() THEN NULL 
		ELSE bdate 
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		ELSE 'N/A'
	END AS gen
FROM 
	bronze.erp_cust_az12;

/*
	Step: 4 - Push the data into silver table layer.
*/

INSERT INTO 
silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
		ELSE cid 
	END cid, 
	CASE	
		WHEN bdate > GETDATE() THEN NULL 
		ELSE bdate 
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		ELSE 'N/A'
	END AS gen
FROM 
	bronze.erp_cust_az12;
