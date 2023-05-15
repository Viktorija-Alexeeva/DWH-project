CREATE OR REPLACE PROCEDURE BL_CL.load_map_products  ()
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

INSERT INTO BL_CL.map_products
SELECT DISTINCT product_srcid , 'bl_cl' AS source_system , 'map_products' AS source_table ,
		product_desc , product_group_srcid, price , is_active ,
		insert_dt , update_dt  
FROM 
	(
	SELECT DISTINCT scs.product_srcid AS product_srcid,
			COALESCE(scs.product_desc, 'N/A') AS product_desc ,
			COALESCE(scs.product_group_id, '-1') AS product_group_srcid , 
			COALESCE(scs.product_price ::DECIMAL(10,2), -1) AS price,
			COALESCE(scs.product_status ::BOOLEAN, 'N') AS is_active,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_consumer_segment.src_consumer_segment scs
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_products t 
						WHERE t.product_srcid = scs.product_srcid 
							AND t.product_desc = scs.product_desc
							AND t.product_group_srcid = scs.product_group_id
							AND t.price = scs.product_price ::DECIMAL(10,2)
							AND t.is_active = scs.product_status ::BOOLEAN
						)	
				AND scs.is_processed = 'N'
		UNION ALL 
	SELECT DISTINCT sos.product_srcid AS product_srcid,
			COALESCE(sos.product_desc, 'N/A') AS product_desc ,
			COALESCE(sos.product_group_id, '-1') AS product_group_srcid , 
			COALESCE(sos.product_price ::DECIMAL(10,2), -1) AS price,
			COALESCE(sos.product_status ::BOOLEAN, 'N') AS is_active,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_office_segment.src_office_segment sos
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_products t 
						WHERE t.product_srcid = sos.product_srcid 
							AND t.product_desc = sos.product_desc
							AND t.product_group_srcid = sos.product_group_id
							AND t.price = sos.product_price ::DECIMAL(10,2)
							AND t.is_active = sos.product_status ::BOOLEAN
						)
				AND sos.is_processed = 'N'
	) q 
GROUP BY product_srcid , source_system , source_table ,	product_desc , 
		product_group_srcid, price , is_active ,
		insert_dt , update_dt  ;
	
-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_CL',     -- INSERT ROW into table log_data if no errors
								'map_products' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'map_products', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
