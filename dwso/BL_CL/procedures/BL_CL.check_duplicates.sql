CREATE OR REPLACE FUNCTION BL_CL.check_duplicates ()
RETURNS TABLE (table_name TEXT,
				dup_found INT)
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
	flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT; 
	ce_address_cities INT;
	ce_address_city_countries INT;
	ce_address_city_country_regions INT;
	ce_addresses INT;
	ce_customers INT;
	ce_employees_scd INT;
	ce_loyal_programs INT;
	ce_markets INT;
	ce_orders INT;
	ce_product_group_categories INT;
	ce_product_groups INT;
	ce_products INT;
	ce_products_suppliers INT;
	ce_sales INT;
	ce_shipments INT;
	ce_supplier_managers INT;
	ce_suppliers INT;
	dim_customers INT;
	dim_employees_scd INT;
	dim_loyal_programs INT;
	dim_markets INT;
	dim_orders INT;
	dim_products INT;
	dim_shipments INT;
	dim_suppliers INT;
	fct_sales INT;

BEGIN 

	SELECT COALESCE(count(*),0) INTO ce_address_cities
	FROM (SELECT count(*) FROM bl_3nf.ce_address_cities
	GROUP BY address_city_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_address_city_countries
	FROM (SELECT count(*) FROM bl_3nf.ce_address_city_countries
	GROUP BY address_city_country_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_address_city_country_regions
	FROM (SELECT count(*) FROM bl_3nf.ce_address_city_country_regions
	GROUP BY address_city_country_region_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_addresses
	FROM (SELECT count(*) FROM bl_3nf.ce_addresses
	GROUP BY address_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_customers
	FROM (SELECT count(*) FROM bl_3nf.ce_customers
	GROUP BY customer_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_employees_scd
	FROM (SELECT count(*) FROM bl_3nf.ce_employees_scd
	GROUP BY employee_id, start_dt
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_loyal_programs
	FROM (SELECT count(*) FROM bl_3nf.ce_loyal_programs
	GROUP BY loyal_program_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_markets
	FROM (SELECT count(*) FROM bl_3nf.ce_markets
	GROUP BY market_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_orders
	FROM (SELECT count(*) FROM bl_3nf.ce_orders
	GROUP BY order_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_product_group_categories
	FROM (SELECT count(*) FROM bl_3nf.ce_product_group_categories
	GROUP BY product_group_category_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_product_groups
	FROM (SELECT count(*) FROM bl_3nf.ce_product_groups
	GROUP BY product_group_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_products
	FROM (SELECT count(*) FROM bl_3nf.ce_products
	GROUP BY product_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_products_suppliers
	FROM (SELECT count(*) FROM bl_3nf.ce_products_suppliers
	GROUP BY product_id, supplier_id
	HAVING count(*)>1) c;	

	SELECT COALESCE(count(*),0) INTO ce_sales
	FROM (SELECT count(*) FROM bl_3nf.ce_sales
	GROUP BY order_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_shipments
	FROM (SELECT count(*) FROM bl_3nf.ce_shipments
	GROUP BY shipment_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_supplier_managers
	FROM (SELECT count(*) FROM bl_3nf.ce_supplier_managers
	GROUP BY manager_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO ce_suppliers
	FROM (SELECT count (*) FROM bl_3nf.ce_suppliers
	GROUP BY supplier_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO dim_customers
	FROM (SELECT count(*) FROM bl_dm.dim_customers
	GROUP BY customer_surr_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO dim_employees_scd
	FROM (SELECT count(*) FROM bl_dm.dim_employees_scd
	GROUP BY employee_id, start_dt
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO dim_loyal_programs
	FROM (SELECT count(*) FROM bl_dm.dim_loyal_programs
	GROUP BY loyal_program_surr_id
	HAVING count(*)>1) c;	

	SELECT COALESCE(count(*), 0) INTO dim_markets
	FROM (SELECT count(*) FROM bl_dm.dim_markets
	GROUP BY market_surr_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*), 0) INTO dim_orders
	FROM (SELECT count(*) FROM bl_dm.dim_orders
	GROUP BY order_surr_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO dim_products
	FROM (SELECT count(*) FROM bl_dm.dim_products
	GROUP BY product_surr_id
	HAVING count(*)>1) c;
	
	SELECT COALESCE(count(*),0) INTO dim_shipments
	FROM (SELECT count(*) FROM bl_dm.dim_shipments
	GROUP BY shipment_surr_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO dim_suppliers
	FROM (SELECT count(*) FROM bl_dm.dim_suppliers
	GROUP BY supplier_surr_id
	HAVING count(*)>1) c;

	SELECT COALESCE(count(*),0) INTO fct_sales
	FROM (SELECT count(*) FROM bl_dm.fct_sales
	GROUP BY order_surr_id
	HAVING count(*)>1) c;

	RETURN query 
	SELECT 'ce_address_cities', ce_address_cities
	UNION 
	SELECT 'ce_address_city_countries', ce_address_city_countries
	UNION 
	SELECT 'ce_address_city_country_regions', ce_address_city_country_regions
	UNION 
	SELECT 'ce_addresses', ce_addresses
	UNION 
	SELECT 'ce_customers', ce_customers
	UNION 
	SELECT 'ce_employees_scd', ce_employees_scd
	UNION 
	SELECT 'ce_loyal_programs', ce_loyal_programs	
	UNION 
	SELECT 'ce_markets', ce_markets	
	UNION 
	SELECT 'ce_orders', ce_orders	
	UNION 
	SELECT 'ce_product_group_categories', ce_product_group_categories	
	UNION 
	SELECT 'ce_product_groups', ce_product_groups
	UNION 
	SELECT 'ce_products', ce_products	
	UNION 
	SELECT 'ce_products_suppliers', ce_products_suppliers
	UNION 
	SELECT 'ce_sales', ce_sales
	UNION 
	SELECT 'ce_shipments', ce_shipments
	UNION 
	SELECT 'ce_supplier_managers', ce_supplier_managers
	UNION 
	SELECT 'ce_suppliers', ce_suppliers
	UNION 
	SELECT 'dim_customers', dim_customers
	UNION
	SELECT 'dim_employees_scd', dim_employees_scd
	UNION
	SELECT 'dim_loyal_programs', dim_loyal_programs
	UNION
	SELECT 'dim_markets', dim_markets
	UNION 
	SELECT 'dim_orders', dim_orders
	UNION 
	SELECT 'dim_products', dim_products
	UNION 
	SELECT 'dim_shipments', dim_shipments
	UNION 
	SELECT 'dim_suppliers', dim_suppliers
	UNION 
	SELECT 'fct_sales', fct_sales;

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'check_duplicates', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
END; 
$$;


SELECT * FROM BL_CL.check_duplicates () ;
