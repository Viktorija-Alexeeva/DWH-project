CREATE OR REPLACE PROCEDURE BL_CL.load_ce_markets_init ()
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

INSERT INTO bl_3nf.ce_markets
SELECT -1, '-1', 'MANUAL', 'MANUAL', 'N/A',
		COALESCE(address_id, -1),
		to_timestamp(1900-01-01) ,
		to_timestamp(1900-01-01)  
FROM bl_3nf.ce_addresses
WHERE address_srcid = '-1'
ON CONFLICT (market_srcid) DO NOTHING;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_markets_def' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_markets_def', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
