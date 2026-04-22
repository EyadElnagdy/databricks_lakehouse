query1="""
        SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE to_date(sls_order_dt,"yyyyMMdd")
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE to_date(sls_ship_dt,"yyyyMMdd")
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE to_date(sls_due_dt,"yyyyMMdd")
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN CAST(sls_sales / NULLIF(sls_quantity, 0) AS DECIMAL(10,2))
			ELSE ROUND(sls_price,2)  -- Derive price if original value is invalid
			END AS sls_price
		FROM sales.bronze.crm_sales_details
"""
query2="""
        SELECT
			prd_id,
			substring(prd_key,7,LEN(prd_key)) prd_key,
			REPLACE(substring(prd_key,1,5),'-','_') cat_id,
			prd_nm,
			prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			prd_start_dt,
			cast(DATEADD(day,-1,LEAD(prd_start_dt,1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) as DATE) AS pd_end_dt
		FROM sales.bronze.crm_prd_info

"""
query3="""
        SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
				ELSE 'n/a'
			END AS cst_marital_status,
			CASE
				WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM
		(
		select *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as rn
		FROM sales.bronze.crm_cust_info
		) AS t
		where rn=1
"""
query4="""
        SELECT
			REPLACE(CID,'NAS','') AS CID,
			CASE
				WHEN BDATE> GETDATE() THEN NULL
				ELSE BDATE
			END AS BDATE,
			CASE
				WHEN UPPER(TRIM(GEN)) IN ('Male','M') THEN 'Male'
				WHEN UPPER(TRIM(GEN)) IN ('Female','F') THEN 'Female'
				ELSE 'n/a'
			END GEN
		FROM sales.bronze.erp_cust_az12
"""
query5="""
        SELECT 
			REPLACE(CID,'-','') AS CID,
			CASE 
				WHEN TRIM(CNTRY) ='DE' THEN 'Germany'
				WHEN TRIM(CNTRY) ='USA' THEN 'United States'
				WHEN TRIM(CNTRY) ='US' THEN 'United States'
				WHEN TRIM(CNTRY) ='' OR TRIM(CNTRY) is NULL THEN 'n/a'
			ELSE TRIM(CNTRY) END as CNTRY
		FROM sales.bronze.erp_loc_a101
"""
query6="""
        SELECT
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM sales.bronze.erp_px_cat_g1v2
"""

TRANSFORMATION = [
    {
        "query": query1,
        "destination": "sales.silver.crm_sales_details"
    },
    {
        "query": query2,
        "destination": "sales.silver.crm_prd_info"
    },
    {
        "query": query3,
        "destination": "sales.silver.crm_cust_info"
    },
    {
        "query": query4,
        "destination": "sales.silver.erp_cust_az12"
    },
    {
        "query": query5,
        "destination": "sales.silver.erp_loc_a101"
    },
    {
        "query": query6,
        "destination": "sales.silver.erp_px_cat_g1v2"
    }
]




