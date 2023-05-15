CREATE OR REPLACE PROCEDURE BL_CL.load_dim_loyal_programs ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
			
INSERT INTO BL_DM.dim_loyal_programs
SELECT CASE 
			WHEN l.loyal_program_id = -1 THEN -1
			ELSE nextval('BL_DM.dim_loyal_programs_seq') 
		END AS loyal_program_surr_id ,
		COALESCE(l.loyal_program_id, -1) AS loyal_program_id,
		COALESCE(l.loyal_program_desc, 'N/A') AS loyal_program_desc,
		COALESCE(l.loyal_discount, -1) AS loyal_discount,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt	
FROM BL_3NF.ce_loyal_programs l
WHERE NOT EXISTS 
				(
				SELECT DISTINCT 1
				FROM BL_DM.dim_loyal_programs t 
				WHERE t.loyal_program_id = l.loyal_program_id 
					AND t.loyal_program_desc = l.loyal_program_desc
					AND t.loyal_discount = l.loyal_discount 
				)
ON CONFLICT (loyal_program_id) DO UPDATE SET  
loyal_program_desc = EXCLUDED.loyal_program_desc,
loyal_discount = EXCLUDED.loyal_discount,
update_dt = current_timestamp 
WHERE dim_loyal_programs.loyal_program_desc != EXCLUDED.loyal_program_desc
	OR dim_loyal_programs.loyal_discount != EXCLUDED.loyal_discount ;

-- get diagnostics
	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_DM', 
								'dim_loyal_programs' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_DM', 
        					'dim_loyal_programs', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
        RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
