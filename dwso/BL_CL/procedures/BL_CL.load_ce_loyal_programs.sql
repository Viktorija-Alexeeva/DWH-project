CREATE OR REPLACE PROCEDURE BL_CL.load_ce_loyal_programs ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert from map_loyal_programs
			
MERGE INTO bl_3nf.ce_loyal_programs loy 
USING bl_cl.map_loyal_programs mlp
ON loy.loyal_program_srcid = mlp.loyal_program_srcid
WHEN MATCHED 
			AND loy.loyal_program_desc != mlp.loyal_program_desc
			OR loy.loyal_discount != mlp.loyal_discount
	THEN 
	UPDATE SET 
		loyal_program_desc = mlp.loyal_program_desc,
		loyal_discount = mlp.loyal_discount,
		update_dt = current_timestamp
WHEN NOT MATCHED THEN
	INSERT 
	VALUES (nextval('BL_3NF.ce_loyal_programs_seq'),
			COALESCE(mlp.loyal_program_srcid, '-1'),
			mlp.source_system,
			mlp.source_table,
			COALESCE(mlp.loyal_program_desc, 'N/A'),
			COALESCE(mlp.loyal_discount, -1),
			current_timestamp ,
			current_timestamp 
			) ;

-- get diagnostics
	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF', 
								'ce_loyal_programs' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF', 
        					'ce_loyal_programs', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
        RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 