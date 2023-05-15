CREATE OR REPLACE PROCEDURE BL_CL.load_map_product_groups  ()
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

INSERT INTO BL_CL.map_product_groups
SELECT DISTINCT product_group_srcid , 'bl_cl' AS source_system , 'map_product_groups' AS source_table ,
		product_group_desc , product_group_category_srcid, insert_dt , update_dt  
FROM 
	(		
	SELECT DISTINCT scs.product_group_id AS product_group_srcid,
			COALESCE(scs.product_group_desc, 'N/A') AS product_group_desc ,
			COALESCE(scs.product_category_id , '-1') AS product_group_category_srcid,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_consumer_segment.src_consumer_segment scs
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_product_groups t 
						WHERE t.product_group_desc = scs.product_group_desc
							AND t.product_group_category_srcid = scs.product_category_id
						)
				AND scs.is_processed = 'N'
		UNION ALL 
	SELECT DISTINCT sos.product_group_id AS product_group_srcid,
			COALESCE(sos.product_group_desc, 'N/A') AS product_group_desc ,
			COALESCE(sos.product_category_id, '-1') AS product_group_category_srcid,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_office_segment.src_office_segment sos
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_product_groups t 
						WHERE t.product_group_desc = sos.product_group_desc
							AND t.product_group_category_srcid = sos.product_category_id
						)
				AND sos.is_processed = 'N'
	) AS q
GROUP BY product_group_srcid , source_system , source_table ,
		product_group_desc , product_group_category_srcid, 
		insert_dt , update_dt ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_CL',     -- INSERT ROW into table log_data if no errors
								'map_product_groups' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'map_product_groups', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
