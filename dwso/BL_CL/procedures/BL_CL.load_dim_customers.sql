CREATE OR REPLACE PROCEDURE BL_CL.load_dim_customers ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
		
INSERT INTO BL_DM.dim_customers 
SELECT 	CASE 
			WHEN cu.customer_id = -1 THEN -1
			ELSE nextval('BL_DM.dim_customers_seq')
		END AS customer_surr_id ,
		COALESCE(cu.customer_id, -1) AS customer_id , 
		COALESCE(cu."name", 'N/A') AS customer_name ,
		COALESCE(cu.date_of_birth, '1900-01-01') AS customer_date_of_birth ,
		COALESCE(cu.full_age, -1) AS customer_full_age ,
		COALESCE(cu.gender, 'N/A') AS customer_gender ,
		COALESCE(cu.email, 'N/A') AS customer_email ,
		COALESCE(cu.phone, 'N/A') AS customer_phone ,
		COALESCE(cu.address_id, -1) AS customer_address_id , 
		COALESCE(ad.address_desc, 'N/A') AS customer_address_desc ,
		COALESCE(ad.address_city_id, -1) AS customer_address_city_id , 
		COALESCE(ci.address_city_desc, 'N/A') AS customer_address_city_desc ,
		COALESCE(ci.address_city_country_id, -1) AS customer_address_city_country_id , 
		COALESCE(cnt.address_city_country_desc, 'N/A') AS customer_address_city_country_desc ,
		COALESCE(cnt.address_city_country_region_id, -1) AS customer_address_city_country_region_id , 
		COALESCE(reg.address_city_country_region_desc, 'N/A') AS customer_address_city_country_region_desc ,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt	
FROM BL_3NF.ce_customers cu
LEFT JOIN bl_3nf.ce_addresses ad
ON  cu.address_id  = ad.address_id  
LEFT JOIN bl_3nf.ce_address_cities ci
ON  ad.address_city_id  = ci.address_city_id 
LEFT JOIN bl_3nf.ce_address_city_countries cnt
ON  ci.address_city_country_id  = cnt.address_city_country_id 
LEFT JOIN bl_3nf.ce_address_city_country_regions reg
ON  cnt.address_city_country_region_id  = reg.address_city_country_region_id 
WHERE NOT EXISTS 
				(
				SELECT DISTINCT 1
				FROM BL_DM.dim_customers t 
				WHERE t.customer_id = cu.customer_id
					AND t.customer_name = cu."name"
					AND t.customer_date_of_birth = cu.date_of_birth
					AND t.customer_full_age = cu.full_age
					AND t.customer_gender = cu.gender 
					AND t.customer_email = cu.email 
					AND t.customer_phone = cu.phone 
					AND t.customer_address_id = cu.address_id
				)
ON CONFLICT (customer_id) DO UPDATE SET  
customer_name = EXCLUDED.customer_name,
customer_date_of_birth = EXCLUDED.customer_date_of_birth,
customer_full_age = EXCLUDED.customer_full_age ,
customer_gender = EXCLUDED.customer_gender ,
customer_email = EXCLUDED.customer_email ,
customer_phone = EXCLUDED.customer_phone ,
customer_address_id = EXCLUDED.customer_address_id ,
customer_address_desc = EXCLUDED.customer_address_desc ,
customer_address_city_id = EXCLUDED.customer_address_city_id ,
customer_address_city_desc = EXCLUDED.customer_address_city_desc ,
customer_address_city_country_id = EXCLUDED.customer_address_city_country_id ,
customer_address_city_country_desc = EXCLUDED.customer_address_city_country_desc ,
customer_address_city_country_region_id = EXCLUDED.customer_address_city_country_region_id ,
customer_address_city_country_region_desc = EXCLUDED.customer_address_city_country_region_desc ,
update_dt = current_timestamp 
WHERE dim_customers.customer_name != EXCLUDED.customer_name
	OR dim_customers.customer_date_of_birth != EXCLUDED.customer_date_of_birth
	OR dim_customers.customer_full_age != EXCLUDED.customer_full_age 
	OR dim_customers.customer_gender != EXCLUDED.customer_gender 
	OR dim_customers.customer_email != EXCLUDED.customer_email 
	OR dim_customers.customer_phone != EXCLUDED.customer_phone 
	OR dim_customers.customer_address_id != EXCLUDED.customer_address_id 
	OR dim_customers.customer_address_desc != EXCLUDED.customer_address_desc 
	OR dim_customers.customer_address_city_id != EXCLUDED.customer_address_city_id 
	OR dim_customers.customer_address_city_desc != EXCLUDED.customer_address_city_desc 
	OR dim_customers.customer_address_city_country_id != EXCLUDED.customer_address_city_country_id 
	OR dim_customers.customer_address_city_country_desc != EXCLUDED.customer_address_city_country_desc 
	OR dim_customers.customer_address_city_country_region_id != EXCLUDED.customer_address_city_country_region_id 
	OR dim_customers.customer_address_city_country_region_desc != EXCLUDED.customer_address_city_country_region_desc ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_DM',     -- INSERT ROW into table log_data if no errors
								'dim_customers' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_DM',      -- INSERT ROW into table log_data if error
        					'dim_customers', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

