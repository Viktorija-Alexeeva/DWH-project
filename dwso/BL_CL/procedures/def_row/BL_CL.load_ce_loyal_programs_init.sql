CREATE OR REPLACE PROCEDURE BL_CL.load_ce_loyal_programs_init ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert default row 

INSERT INTO bl_3nf.ce_loyal_programs
SELECT -1, '-1', 'MANUAL', 'MANUAL', 
		'N/A', -99, 
		to_timestamp(1900-01-01) ,
		to_timestamp(1900-01-01) 
ON CONFLICT (loyal_program_srcid) DO NOTHING;

-- get diagnostics
	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF', 
								'ce_loyal_programs_def' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF', 
        					'ce_loyal_programs_def', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
        RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 