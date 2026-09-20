SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price 
FROM 
	bronze.crm_sales_details;

/*
	Step-1: 
		Checking duplicates and NULL's in the sls_ord_num 
		Result - No Duplicates.
	
	Step-2:
		Check the integrity of the columns as we join with the customer_info and product_info tables
*/

SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price 
FROM 
	bronze.crm_sales_details 
WHERE 
	sls_prd_key NOT IN 
(
	SELECT 
		prd_key 
	FROM 
		silver.crm_prd_info
);

/*
	Step: 3 Check for Invalid dates 
		Negative numbers or zeros cant be cast to a date.
		In this scenario, the length of the date must be 8
		Check the outliers by validating the boundaries of the date range.
*/

SELECT 
	NULLIF(sls_order_dt, 0) as sls_order_dt
FROM 
	bronze.crm_sales_details 
WHERE 
	sls_order_dt <= 0 
OR len(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101;

SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
	END AS sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price 
FROM 
	bronze.crm_sales_details;

/*	
	Step 4: Order date must be lesser than the shipping date.
*/

SELECT 
	* 
FROM 
	bronze.crm_sales_details 
WHERE 
	sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

/*
	Step: 5 - Business Rules 
		Sum of sales = Price * Quantity 
		Negatives, zeroes, NULLs are not allowed.

	Rules:
		If Sales is negative, zero or null, derive it using Quantity and Price.
		If Price is zero or null, calculate it using sales and Quantity.
		If Price is negative, convert it into a Positive value.

*/

SELECT 
	sls_sales AS sls_old_sales,
	sls_quantity,
	sls_price AS sls_old_price,
	CASE 
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales 
	END AS sls_sales,
	CASE 
		WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price 
	END AS sls_price
FROM 
	bronze.crm_sales_details 
WHERE 
	sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales 
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price 
	END AS sls_price
FROM 
	bronze.crm_sales_details;

/*
	Step: 6 - Create new product info table 
*/

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

/*
	Step: 8 - Insert the data into silver layer table
*/

INSERT INTO silver.crm_sales_details
(
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
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales 
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price 
	END AS sls_price
FROM 
	bronze.crm_sales_details;
