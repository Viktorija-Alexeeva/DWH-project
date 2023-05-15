CREATE OR REPLACE PROCEDURE BL_CL.load_ce_markets  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert from map_markets
			
INSERT INTO bl_3nf.ce_markets 
SELECT nextval('BL_3NF.ce_markets_seq') AS market_id,
		COALESCE(mar.market_srcid, '-1') AS market_srcid ,
		mar.source_system AS source_system ,
		mar.source_table AS source_table ,
		COALESCE(mar.market_desc, 'N/A') AS market_desc  ,
		COALESCE(ad.address_id, -1) AS address_id,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt 
FROM BL_CL.map_markets mar
LEFT JOIN bl_3nf.ce_addresses ad
ON mar.address_desc = ad.address_desc 
WHERE NOT EXISTS (
					SELECT DISTINCT 1
					FROM bl_3nf.ce_markets t 
					WHERE t.market_desc = mar.market_desc
					)
ON CONFLICT (market_srcid) DO UPDATE SET 
market_desc = EXCLUDED.market_desc,
address_id = EXCLUDED.address_id,
update_dt = current_timestamp 
WHERE ce_markets.market_desc != EXCLUDED.market_desc
	OR ce_markets.address_id != EXCLUDED.address_id ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_markets' ,
								diag_row_count, 
								flag);
							
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_markets', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
