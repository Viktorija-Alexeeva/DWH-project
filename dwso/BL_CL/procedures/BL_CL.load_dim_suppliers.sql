CREATE OR REPLACE PROCEDURE BL_CL.load_dim_suppliers ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
			
INSERT INTO BL_DM.dim_suppliers 
SELECT CASE 
			WHEN s.supplier_id = -1 THEN -1
			ELSE nextval('BL_DM.dim_suppliers_seq')
		END  AS supplier_surr_id ,
		COALESCE(s.supplier_id, -1) AS supplier_id , 
		COALESCE(s."name", 'N/A') AS supplier_name ,
		COALESCE(m.manager_id, -1) AS supplier_manager_id, 
		COALESCE(m.manager_name, 'N/A') AS supplier_manager_name ,
		COALESCE(m.manager_phone, 'N/A') AS supplier_manager_phone ,
		COALESCE(m.manager_email, 'N/A') AS supplier_manager_email ,
		COALESCE(s.address_id, -1) AS supplier_address_id , 
		COALESCE(ad.address_desc, 'N/A') AS supplier_address_desc ,
		COALESCE(ad.address_city_id, -1) AS supplier_address_city_id , 
		COALESCE(ci.address_city_desc, 'N/A') AS supplier_address_city_desc ,
		COALESCE(ci.address_city_country_id, -1) AS supplier_address_city_country_id , 
		COALESCE(cnt.address_city_country_desc, 'N/A') AS supplier_address_city_country_desc ,
		COALESCE(cnt.address_city_country_region_id, -1) AS supplier_address_city_country_region_id , 
		COALESCE(reg.address_city_country_region_desc, 'N/A') AS supplier_address_city_country_region_desc ,
		COALESCE(s.is_active, 'N') AS is_active ,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt	
FROM BL_3NF.ce_suppliers s
LEFT JOIN bl_3nf.ce_supplier_managers m 
ON m.supplier_id = s.supplier_id 
LEFT JOIN bl_3nf.ce_addresses ad
ON  s.address_id  = ad.address_id  
LEFT JOIN bl_3nf.ce_address_cities ci
ON  ad.address_city_id  = ci.address_city_id 
LEFT JOIN bl_3nf.ce_address_city_countries cnt
ON  ci.address_city_country_id  = cnt.address_city_country_id 
LEFT JOIN bl_3nf.ce_address_city_country_regions reg
ON  cnt.address_city_country_region_id  = reg.address_city_country_region_id 
WHERE NOT EXISTS
				(
				SELECT DISTINCT 1
				FROM BL_DM.dim_suppliers t 
				WHERE t.supplier_id = s.supplier_id
					AND t.supplier_name = s."name"
					AND t.supplier_manager_id = m.manager_id
					AND t.supplier_manager_name = m.manager_name
					AND t.supplier_manager_phone = m.manager_phone 
					AND t.supplier_manager_email = m.manager_email 
					AND t.supplier_address_id = s.address_id
				)
ON CONFLICT (supplier_id) DO --NOTHING ;
UPDATE SET  
supplier_name = EXCLUDED.supplier_name,
supplier_manager_id = EXCLUDED.supplier_manager_id,
supplier_manager_name = EXCLUDED.supplier_manager_name ,
supplier_manager_phone = EXCLUDED.supplier_manager_phone ,
supplier_manager_email = EXCLUDED.supplier_manager_email ,
supplier_address_id = EXCLUDED.supplier_address_id ,
supplier_address_desc = EXCLUDED.supplier_address_desc ,
supplier_address_city_id = EXCLUDED.supplier_address_city_id ,
supplier_address_city_desc = EXCLUDED.supplier_address_city_desc ,
supplier_address_city_country_id = EXCLUDED.supplier_address_city_country_id ,
supplier_address_city_country_desc = EXCLUDED.supplier_address_city_country_desc ,
supplier_address_city_country_region_id = EXCLUDED.supplier_address_city_country_region_id ,
supplier_address_city_country_region_desc = EXCLUDED.supplier_address_city_country_region_desc ,
update_dt = current_timestamp 
WHERE dim_suppliers.supplier_name != EXCLUDED.supplier_name
	OR dim_suppliers.supplier_manager_id != EXCLUDED.supplier_manager_id
	OR dim_suppliers.supplier_manager_name != EXCLUDED.supplier_manager_name
	OR dim_suppliers.supplier_manager_phone != EXCLUDED.supplier_manager_phone
	OR dim_suppliers.supplier_manager_email != EXCLUDED.supplier_manager_email
	OR dim_suppliers.supplier_address_id != EXCLUDED.supplier_address_id
	OR dim_suppliers.supplier_address_desc != EXCLUDED.supplier_address_desc
	OR dim_suppliers.supplier_address_city_id != EXCLUDED.supplier_address_city_id
	OR dim_suppliers.supplier_address_city_desc != EXCLUDED.supplier_address_city_desc
	OR dim_suppliers.supplier_address_city_country_id != EXCLUDED.supplier_address_city_country_id
	OR dim_suppliers.supplier_address_city_country_desc != EXCLUDED.supplier_address_city_country_desc
	OR dim_suppliers.supplier_address_city_country_region_id != EXCLUDED.supplier_address_city_country_region_id
	OR dim_suppliers.supplier_address_city_country_region_desc != EXCLUDED.supplier_address_city_country_region_desc ;


-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_DM',     -- INSERT ROW into table log_data if no errors
								'dim_suppliers' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_DM',      -- INSERT ROW into table log_data if error
        					'dim_suppliers', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

