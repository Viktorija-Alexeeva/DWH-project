CREATE OR REPLACE PROCEDURE BL_CL.load_ce_orders  ()
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

WITH q AS (
			SELECT DISTINCT nextval('BL_3NF.ce_orders_seq') AS order_id,
					COALESCE(scs.order_srcid, '-1') AS order_srcid ,
					'sa_consumer_segment' AS source_system, 
					'src_consumer_segment' AS source_table,
					COALESCE(scs.order_priority, 'N/A') AS order_priority ,
					COALESCE(scs.order_date ::DATE, '1900-01-01') AS order_date ,		
					current_timestamp AS insert_dt,
					current_timestamp AS update_dt 
			FROM sa_consumer_segment.src_consumer_segment scs
			WHERE scs.is_processed = 'N'
				UNION 
			SELECT DISTINCT nextval('BL_3NF.ce_orders_seq') AS order_id,
					COALESCE(sos.order_srcid, '-1') AS order_srcid ,
					'sa_consumer_segment' AS source_system, 
					'src_consumer_segment' AS source_table,
					COALESCE(sos.order_priority, 'N/A') AS order_priority ,
					COALESCE(sos.order_date ::DATE, '1900-01-01') AS order_date ,		
					current_timestamp AS insert_dt,
					current_timestamp AS update_dt 
			FROM sa_office_segment.src_office_segment sos
			WHERE sos.is_processed = 'N'
			)
MERGE INTO bl_3nf.ce_orders ord  
USING q 
ON ord.order_srcid = q.order_srcid
WHEN MATCHED
			AND ord.order_priority != q.order_priority
			OR 	ord.order_date != q.order_date
	THEN 
	UPDATE SET 
		order_priority = q.order_priority,
		order_date = q.order_date,
		update_dt = current_timestamp
WHEN NOT MATCHED THEN
	INSERT 
	VALUES (nextval('BL_3NF.ce_orders_seq'),
			COALESCE(q.order_srcid, '-1'),
			q.source_system,
			q.source_table,
			COALESCE(q.order_priority, 'N/A'),
			COALESCE(q.order_date, '1900-01-01'),
			current_timestamp ,
			current_timestamp 
			) ;			
		
-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_orders' ,
								diag_row_count, 
								flag);	

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_orders', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
