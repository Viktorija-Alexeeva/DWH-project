CREATE OR REPLACE PROCEDURE BL_CL.load_map_markets  ()
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
INSERT INTO BL_CL.map_markets
SELECT DISTINCT market_srcid , 'bl_cl' AS source_system , 'map_markets' AS source_table ,
		market_desc, address_desc, insert_dt , update_dt  
FROM 
	(				
	SELECT DISTINCT scs.market_srcid AS market_srcid,
			COALESCE(scs.market_desc, 'N/A') AS market_desc  ,
			COALESCE(scs.market_address, 'N/A') AS address_desc,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_consumer_segment.src_consumer_segment scs
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_markets t 
						WHERE t.market_desc = scs.market_desc
							AND t.address_desc = scs.market_address
						)
				AND scs.is_processed = 'N'
			UNION ALL
	SELECT DISTINCT sos.market_srcid AS market_srcid,
			COALESCE(sos.market_desc, 'N/A') AS market_desc  ,
			COALESCE(sos.market_address, 'N/A') AS address_desc,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_office_segment.src_office_segment sos
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_markets t 
						WHERE t.market_desc = sos.market_desc
							AND t.address_desc = sos.market_address
						)
				AND sos.is_processed = 'N'
	) AS q
GROUP BY market_srcid , source_system , source_table ,
		market_desc, address_desc, insert_dt , update_dt  ;				

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_CL',     -- INSERT ROW into table log_data if no errors
								'map_markets' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'map_markets', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
