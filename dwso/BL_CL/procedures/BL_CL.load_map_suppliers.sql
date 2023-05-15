CREATE OR REPLACE PROCEDURE BL_CL.load_map_suppliers  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert from src_consumer_segment and src_office_segment

INSERT INTO BL_CL.map_suppliers
SELECT DISTINCT supplier_srcid , 'bl_cl' AS source_system , 'map_suppliers' AS source_table ,
		"name" , address_desc, is_active , insert_dt , update_dt  
FROM 
	(			
	SELECT DISTINCT scs.supplier_srcid AS supplier_srcid  ,
			COALESCE(scs.supplier_name, 'N/A') AS "name" ,
			COALESCE(scs.supplier_address , 'N/A') AS address_desc ,
			scs.supplier_status::BOOLEAN AS is_active,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_consumer_segment.src_consumer_segment scs
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_suppliers t 
						WHERE t."name" = scs.supplier_name
							AND t.address_desc = scs.supplier_address
							AND t.is_active = scs.supplier_status ::BOOLEAN
						)
				AND scs.is_processed = 'N'
		UNION ALL 
	SELECT DISTINCT sos.supplier_srcid AS supplier_srcid  ,
			COALESCE(sos.supplier_name, 'N/A') AS "name" ,
			COALESCE(sos.supplier_address , 'N/A') AS address_desc ,
			sos.supplier_status::BOOLEAN AS is_active,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_office_segment.src_office_segment sos
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_suppliers t 
						WHERE t."name" = sos.supplier_name
							AND t.address_desc = sos.supplier_address
							AND t.is_active = sos.supplier_status ::BOOLEAN
						)
				AND sos.is_processed = 'N'
	) AS q
GROUP BY supplier_srcid , source_system , source_table , "name" , 
			address_desc, is_active , insert_dt , update_dt ; 

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_CL',     -- INSERT ROW into table log_data if no errors
								'map_suppliers' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'map_suppliers', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

