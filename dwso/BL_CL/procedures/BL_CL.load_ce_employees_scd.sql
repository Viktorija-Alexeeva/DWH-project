CREATE OR REPLACE PROCEDURE BL_CL.load_ce_employees_scd ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 

-- update existing rows from bl_cl.map_employees

	flag = 'I/U';
	
WITH all_employees AS 
					(
INSERT INTO bl_3nf.ce_employees_scd
SELECT  COALESCE(t.employee_id, nextval('BL_3NF.ce_employees_scd_seq')) AS employee_id,
		COALESCE(mem.employee_srcid, '-1') AS employee_srcid ,
		mem.source_system AS source_system , 
		mem.source_table AS source_table ,
		COALESCE(mem."name", 'N/A') AS "name" ,
		COALESCE(mem.surname, 'N/A') AS surname ,
		mem."name" || ' ' || mem.surname AS full_name,
		COALESCE(mem.date_of_birth, '1900-01-01') AS date_of_birth ,
		COALESCE(mem.email , 'N/A') AS email ,
		COALESCE(mem.phone , 'N/A') AS phone ,
		COALESCE(mar.market_id, -1) AS market_id,
		COALESCE(mem."position", 'N/A') AS "position" ,
		CASE 
			WHEN t.employee_id IS NOT NULL 
			THEN current_timestamp
			ELSE '1900-01-01'
		END  AS start_dt,
		'2099-12-31'::TIMESTAMP AS end_dt,
		'Y'::BOOLEAN AS is_active,
		current_timestamp AS insert_dt 
FROM bl_cl.map_employees mem  
LEFT JOIN  bl_3nf.ce_employees_scd t 
ON mem.employee_srcid = t.employee_srcid 
LEFT JOIN bl_3nf.ce_markets mar
ON mem.market_srcid = mar.market_srcid  
WHERE NOT EXISTS (
				SELECT DISTINCT 1 
				FROM bl_3nf.ce_employees_scd t2 
				WHERE mem.employee_srcid = t2.employee_srcid 
					AND t2.full_name = mem.full_name
					AND t2.date_of_birth = mem.date_of_birth 
					AND t2.email = mem.email
					AND t2.phone = mem.phone
					AND t2.market_id = mar.market_id 
					AND t2."position" = mem."position"
				)

ON CONFLICT (employee_id, start_dt) DO NOTHING
RETURNING *)				

UPDATE bl_3nf.ce_employees_scd t    
SET is_active = 'N', 
	end_dt = current_timestamp 
WHERE t.employee_srcid IN (
							SELECT ae.employee_srcid
							FROM all_employees ae
							WHERE t.employee_srcid = ae.employee_srcid
							)	
AND t.is_active = 'Y'
AND t.start_dt < current_timestamp ; 	


-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- for insert statement INSERT ROW into table log_data if no errors
								'ce_employees_scd' ,
								diag_row_count, 
								flag);
							
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_employees_scd', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

