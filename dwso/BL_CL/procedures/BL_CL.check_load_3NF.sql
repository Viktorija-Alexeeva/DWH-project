CREATE OR REPLACE PROCEDURE BL_CL.check_load_3NF ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
	diag_row_count1 INT;
	diag_row_count2 INT;
	S1 BIGINT;

BEGIN 
	flag = 'U';
	-- check number of rows from 2 src tables and ce_sales. if equal - insert was correct, can update is_processed flag 
	S1 = (
		SELECT COALESCE(count(*), 0)   
		FROM (
				SELECT DISTINCT order_srcid
			FROM sa_consumer_segment.src_consumer_segment  

				UNION  

			SELECT DISTINCT order_srcid
			FROM sa_office_segment.src_office_segment  

				EXCEPT 

			SELECT DISTINCT ord.order_srcid  
			FROM bl_3nf.ce_sales s 
			LEFT JOIN bl_3nf.ce_orders ord
			ON s.order_id = ord.order_id 
			) AS q
		);
	
		IF S1 = 0 THEN 
	
	--UPDATING SOURCE TABLES
	UPDATE sa_consumer_segment.src_consumer_segment 
	SET is_processed = 'Y'
	WHERE is_processed = 'N';
	GET DIAGNOSTICS diag_row_count1 = ROW_COUNT ;
	
	UPDATE sa_office_segment.src_office_segment 
	SET is_processed = 'Y'
	WHERE is_processed = 'N';
	GET DIAGNOSTICS diag_row_count2 = ROW_COUNT ;
	diag_row_count = diag_row_count1 + diag_row_count2;
		CALL BL_CL.load_log_data('BL_CL',     -- INSERT ROW into table log_data if no errors
								'check_load_3NF' ,
								diag_row_count, 
								flag);
	
	ELSE   -- INSERT ROW INTO TABLE log_data IF INSERT incorrect
	INSERT INTO bl_cl.log_data ( schema_name, table_name, flag, error_message)
	VALUES ('BL_CL', 'src_tables', 'E', 'INCORRECT INSERT FROM SRC_TABLES TO BL_3NF');
	
	END IF;

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'check_load_3NF', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

