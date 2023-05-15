CREATE OR REPLACE PROCEDURE BL_CL.load_map_loyal_programs ()
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
			
INSERT INTO BL_CL.map_loyal_programs
SELECT DISTINCT loyal_program_srcid , 'bl_cl' AS source_system , 'map_loyal_programs' AS source_table ,
		loyal_program_desc , loyal_discount, insert_dt , update_dt  
FROM 
	(	
	SELECT DISTINCT scs.loyal_program_srcid AS loyal_program_srcid ,
			COALESCE(scs.loyal_program_desc, 'N/A') AS loyal_program_desc  ,
			COALESCE(scs.loyal_discount::DECIMAL(5,3), -1) AS loyal_discount  ,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_consumer_segment.src_consumer_segment scs
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_loyal_programs t 
						WHERE t.loyal_program_desc = scs.loyal_program_desc
							AND t.loyal_discount = scs.loyal_discount::DECIMAL(5,3)
						)
						
				AND scs.is_processed = 'N'
			UNION ALL 
	SELECT DISTINCT sos.loyal_program_srcid AS loyal_program_srcid ,
			COALESCE(sos.loyal_program_desc, 'N/A') AS loyal_program_desc  ,
			COALESCE(sos.loyal_discount::DECIMAL(5,3), -1) AS loyal_discount  ,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_office_segment.src_office_segment sos
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_loyal_programs t 
						WHERE t.loyal_program_desc = sos.loyal_program_desc
							AND t.loyal_discount = sos.loyal_discount::DECIMAL(5,3)
						)
				AND sos.is_processed = 'N'
	) AS q
GROUP BY loyal_program_srcid , source_system , source_table ,
		loyal_program_desc , loyal_discount, insert_dt , update_dt  ;

-- get diagnostics
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_CL', 
								'map_loyal_programs' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL', 
        					'map_loyal_programs', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
        RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 