CREATE OR REPLACE PROCEDURE BL_CL.MASTER_PROC_LOAD ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 

CALL BL_CL.load_src_consumer_segment () ; 
CALL BL_CL.load_src_office_segment () ; 

CALL BL_CL.load_map_employees () ; 
CALL BL_CL.load_map_product_group_categories (); 
CALL BL_CL.load_map_product_groups () ; 
CALL BL_CL.load_map_products  () ; 
CALL BL_CL.load_map_suppliers () ; 
CALL BL_CL.load_map_supplier_managers () ; 
CALL BL_CL.load_map_loyal_programs () ; 
CALL BL_CL.load_map_customers () ; 
CALL BL_CL.load_map_markets () ; 

CALL BL_CL.load_ce_address_city_country_regions () ; 
CALL BL_CL.load_ce_address_city_countries () ; 
CALL BL_CL.load_ce_address_cities (); 
CALL BL_CL.load_ce_addresses () ; 
CALL BL_CL.load_ce_product_group_categories () ; 
CALL BL_CL.load_ce_product_groups () ; 
CALL BL_CL.load_ce_products () ; 
CALL BL_CL.load_ce_suppliers () ; 
CALL BL_CL.load_ce_supplier_managers () ; 
CALL BL_CL.load_ce_products_suppliers () ; 
CALL BL_CL.load_ce_loyal_programs () ; 
CALL BL_CL.load_ce_shipments () ; 
CALL BL_CL.load_ce_orders () ;  
CALL BL_CL.load_ce_customers () ;  
CALL BL_CL.load_ce_markets () ; 
CALL BL_CL.load_ce_employees_scd () ; 
CALL BL_CL.load_ce_sales () ; 

CALL BL_CL.check_load_3nf();

CALL BL_CL.load_dim_customers() ;
CALL BL_CL.load_dim_employees_scd () ;
CALL BL_CL.load_dim_loyal_programs () ;
CALL BL_CL.load_dim_markets () ;
CALL BL_CL.load_dim_orders () ;
CALL BL_CL.load_dim_products () ;
CALL BL_CL.load_dim_shipments () ;
CALL BL_CL.load_dim_suppliers () ;
CALL BL_CL.load_fct_sales () ;

							
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_DM',      -- INSERT ROW into table log_data if error
        					'DIM_TABLES and FCT_TABLE', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
END;
$$ 

CALL BL_CL.MASTER_PROC_LOAD () ;
