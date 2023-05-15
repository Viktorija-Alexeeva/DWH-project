CREATE OR REPLACE PROCEDURE BL_CL.load_ce_product_group_categories  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert from map_product_group_categories 
			
INSERT INTO bl_3nf.ce_product_group_categories 
SELECT nextval('BL_3NF.ce_product_group_categories_seq') AS product_group_category_id,
		COALESCE(mc.product_group_category_srcid, '-1') AS product_group_category_srcid,
		mc.source_system AS source_system , 
		mc.source_table AS source_table ,
		COALESCE(mc.product_group_category_desc, 'N/A') AS product_group_category_desc,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt 
FROM bl_cl.map_product_group_categories mc
WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM bl_3nf.ce_product_group_categories t 
						WHERE t.product_group_category_desc = mc.product_group_category_desc 
						)
ON CONFLICT (product_group_category_srcid) DO UPDATE SET 
product_group_category_desc = EXCLUDED.product_group_category_desc,
update_dt = current_timestamp 
WHERE ce_product_group_categories.product_group_category_desc != EXCLUDED.product_group_category_desc ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_product_group_categories' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_product_group_categories', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
