CREATE OR REPLACE PROCEDURE BL_CL.load_map_supplier_managers  ()
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

INSERT INTO BL_CL.map_supplier_managers
SELECT DISTINCT manager_srcid , 'bl_cl' AS source_system , 'map_supplier_managers' AS source_table ,
		manager_name  , manager_phone , manager_email ,  supplier_srcid , insert_dt , update_dt 
FROM 
	(
	SELECT DISTINCT scs.manager_srcid AS manager_srcid ,
			COALESCE(scs.supplier_manager_name, 'N/A') AS manager_name ,
			COALESCE(scs.supplier_manager_phone, 'N/A') AS manager_phone ,
			COALESCE(scs.supplier_manager_email, 'N/A') AS manager_email ,
			COALESCE(scs.supplier_srcid, '-1') AS supplier_srcid,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt  
	FROM sa_consumer_segment.src_consumer_segment scs
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_supplier_managers t 
						WHERE t.manager_name = scs.supplier_manager_name
							AND t.manager_phone = scs.supplier_manager_phone
							AND t.manager_email = scs.supplier_manager_email
						)		
				AND scs.is_processed = 'N'
		UNION ALL  
	SELECT DISTINCT sos.manager_srcid AS manager_srcid ,
			COALESCE(sos.supplier_manager_name, 'N/A') AS manager_name ,
			COALESCE(sos.supplier_manager_phone, 'N/A') AS manager_phone ,
			COALESCE(sos.supplier_manager_email, 'N/A') AS manager_email ,
			COALESCE(sos.supplier_srcid, '-1') AS supplier_srcid,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_office_segment.src_office_segment sos
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_supplier_managers t 
						WHERE t.manager_name = sos.supplier_manager_name
							AND t.manager_phone = sos.supplier_manager_phone
							AND t.manager_email = sos.supplier_manager_email
						)
				AND sos.is_processed = 'N'
	) AS q
GROUP BY manager_srcid , source_system , source_table , manager_name, 
		manager_phone , manager_email , supplier_srcid, insert_dt , update_dt	;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_CL',     -- INSERT ROW into table log_data if no errors
								'map_supplier_managers' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'map_supplier_managers', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
