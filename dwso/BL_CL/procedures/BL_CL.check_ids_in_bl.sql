CREATE OR REPLACE FUNCTION BL_CL.check_ids_in_bl ()
RETURNS TABLE (src_table TEXT ,
                ce_table TEXT ,
                dm_table TEXT ,
                no_missing_ids BOOLEAN  )
LANGUAGE plpgsql
AS $$

DECLARE 
	diag_row_count INT;
	flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
	_ce_keys       BIGINT;
    _dim_keys      BIGINT;	

BEGIN 
-- bl_3nf
	SELECT  COALESCE(count(*), 0)
	INTO _ce_keys
	FROM (SELECT DISTINCT order_srcid
			FROM sa_consumer_segment.src_consumer_segment  

				UNION  

			SELECT DISTINCT order_srcid
			FROM sa_office_segment.src_office_segment  

				EXCEPT 

			SELECT DISTINCT ord.order_srcid  
			FROM bl_3nf.ce_sales s 
			LEFT JOIN bl_3nf.ce_orders ord
			ON s.order_id = ord.order_id ) q;

-- bl_dm
	SELECT  COALESCE(count(*), 0) 
	INTO _dim_keys
	FROM (SELECT DISTINCT order_id
			FROM bl_3nf.ce_sales  

				EXCEPT 

			SELECT DISTINCT ord.order_id  
			FROM bl_dm.fct_sales s 
			LEFT JOIN bl_dm.dim_orders ord
			ON s.order_surr_id = ord.order_surr_id ) q;		
		
	RETURN  query SELECT  'src_tables', 'ce_sales', 'fct_sales', _ce_keys = 0 and _dim_keys = 0;
	
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'check_ids_in_bl', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
	
END; 
$$;


SELECT * FROM BL_CL.check_ids_in_bl () ;