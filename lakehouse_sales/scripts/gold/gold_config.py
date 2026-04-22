query1="""
    SELECT
        ROW_NUMBER() OVER(ORDER BY a.prd_key, prd_start_dt) product_key,
        a.prd_id product_id,
        a.prd_key product_number,
        a.prd_nm product_name,
        a.cat_id,
        a.prd_line product_line,
        b.CAT category,
        b.SUBCAT subcategory,
        b.MAINTENANCE maintenance,
        a.prd_cost cost,
        a.prd_start_dt start_date
    FROM sales.silver.crm_prd_info a
    LEFT JOIN sales.silver.erp_px_cat_g1v2 b
    ON a.cat_id=b.ID
    where pd_end_dt is null
"""
query2="""
    SELECT
        ROW_NUMBER() OVER(ORDER BY a.cst_key,a.cst_create_date) customer_key,
        a.cst_id customer_id,
        a.cst_key customer_number,
        a.cst_firstname first_name,
        a.cst_lastname last_name,
        a.cst_marital_status marital_status,
        CASE
            WHEN a.cst_gndr <> 'n/a' THEN a.cst_gndr
            ELSE COALESCE(c.GEN,'n/a')
        END AS gender,
        CNTRY country,
        BDATE birth_date,
        a.cst_create_date create_date
    FROM sales.silver.crm_cust_info a
    LEFT JOIN sales.silver.erp_loc_A101 b
    ON a.cst_key =b.CID
    LEFT JOIN sales.silver.erp_CUST_AZ12 c
    ON a.cst_key=c.CID
"""
query3="""
    SELECT
        a.sls_ord_num order_number,
        b.product_key product_key,
        c.customer_key customer_key,
        a.sls_order_dt order_date,
        a.sls_ship_dt shipping_date,
        a.sls_due_dt due_date,
        a.sls_sales sales_amount,
        a.sls_quantity quantity,
        a.sls_price price
    FROM sales.silver.crm_sales_details a
    LEFT JOIN sales.gold.dim_products b
    ON a.sls_prd_key=b.product_number
    LEFT JOIN sales.gold.dim_customers c
    ON a.sls_cust_id=c.customer_id
"""


TRANSFORMATION = [
    {
        "query": query1,
        "destination": "sales.gold.dim_products"
    },
    {
        "query": query2,
        "destination": "sales.gold.dim_customers"
    },
    {
        "query": query3,
        "destination": "sales.gold.fact_sales"
    }
]

