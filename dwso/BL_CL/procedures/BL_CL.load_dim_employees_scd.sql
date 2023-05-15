CREATE OR REPLACE PROCEDURE BL_CL.load_dim_employees_scd ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
						
	flag = 'I/U';	

WITH all_employees AS 
					(
INSERT INTO bl_dm.dim_employees_scd   
SELECT  CASE 
			WHEN e.employee_id = -1 THEN -1
			ELSE nextval('BL_DM.dim_employees_scd_seq')
		END  AS employee_surr_id ,
		COALESCE(e.employee_id, -1) AS employee_id , 
		COALESCE(e."name", 'N/A') AS employee_name ,
		COALESCE(e.surname, 'N/A') AS employee_surname ,
		COALESCE(e.full_name, 'N/A') AS employee_full_name,
		COALESCE(e.date_of_birth, '1900-01-01') AS employee_date_of_birth ,
		COALESCE(e.email, 'N/A') AS employee_email ,
		COALESCE(e.phone, 'N/A') AS employee_phone ,
		COALESCE(e."position", 'N/A') AS employee_position ,
		COALESCE(e.start_dt, '1900-01-01') AS start_dt ,
		COALESCE(e.end_dt, '1900-01-01') AS end_dt ,
		COALESCE(e.is_active, 'N') AS is_active ,
		current_timestamp AS insert_dt  
FROM bl_3nf.ce_employees_scd e  			  		
WHERE NOT EXISTS (
				SELECT DISTINCT 1 
				FROM bl_dm.dim_employees_scd t 
				WHERE t.employee_id = e.employee_id  
					AND t.employee_full_name = e.full_name
					AND t.employee_date_of_birth = e.date_of_birth 
					AND t.employee_email = e.email
					AND t.employee_phone = e.phone
					AND t.employee_position = e."position"
					) 

ON CONFLICT  DO NOTHING
RETURNING *)				

UPDATE bl_dm.dim_employees_scd t    
SET is_active = 'N', 
	end_dt = current_timestamp 
WHERE t.employee_id IN (
							SELECT ae.employee_id
							FROM all_employees ae
							WHERE t.employee_id = ae.employee_id
							)
AND t.is_active = 'Y'
AND t.start_dt < current_timestamp ; 	

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_DM',     -- for insert statement INSERT ROW into table log_data if no errors
								'dim_employees_scd' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_DM',      -- INSERT ROW into table log_data if error
        					'dim_employees_scd', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
