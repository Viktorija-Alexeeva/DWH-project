CREATE OR REPLACE PROCEDURE BL_CL.load_ce_supplier_managers  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert from map_supplier_managers
			
INSERT INTO bl_3nf.ce_supplier_managers 
SELECT nextval('BL_3NF.ce_supplier_managers_seq') AS manager_id,
		COALESCE(msm.manager_srcid, '-1') AS manager_srcid,
		msm.source_system AS source_system  ,
		msm.source_table AS source_table ,
		COALESCE(msm.manager_name, 'N/A') AS manager_name,
		COALESCE(msm.manager_phone, 'N/A') AS manager_phone ,
		COALESCE(msm.manager_email, 'N/A') AS manager_email ,
		COALESCE(sup.supplier_id, -1) AS supplier_id,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt 
FROM  bl_cl.map_supplier_managers msm 
LEFT JOIN bl_3nf.ce_suppliers sup
ON msm.supplier_srcid  = sup.supplier_srcid  
WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM bl_3nf.ce_supplier_managers t
						WHERE t.manager_srcid = msm.manager_srcid 
							AND t.manager_name = msm.manager_name
							AND t.manager_phone = msm.manager_phone
							AND t.manager_email = msm.manager_email
						)		 		
ON CONFLICT (manager_srcid) DO UPDATE SET 
manager_name = EXCLUDED.manager_name, 
manager_phone = EXCLUDED.manager_phone,
manager_email = EXCLUDED.manager_email,
supplier_id = EXCLUDED.supplier_id,
update_dt = current_timestamp 
WHERE ce_supplier_managers.manager_name != EXCLUDED.manager_name
	OR ce_supplier_managers.manager_phone != EXCLUDED.manager_phone
	OR ce_supplier_managers.manager_email != EXCLUDED.manager_email  
	OR ce_supplier_managers.supplier_id != EXCLUDED.supplier_id;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_supplier_managers' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_supplier_managers', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
