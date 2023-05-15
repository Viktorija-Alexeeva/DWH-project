CREATE OR REPLACE PROCEDURE BL_CL.load_ce_products  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert from map_products

INSERT INTO bl_3nf.ce_products 
SELECT DISTINCT nextval('BL_3NF.ce_products_seq') AS product_id,
		COALESCE(mp.product_srcid, '-1') AS product_srcid,
		mp.source_system AS source_system,
		mp.source_table AS source_table,
		COALESCE(mp.product_desc, 'N/A') AS product_desc, 
		COALESCE(gr.product_group_id, -1) AS product_group_id, 
		COALESCE(mp.price, -1) AS price,
		COALESCE(mp.is_active, 'N') AS is_active,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt 
FROM BL_CL.map_products mp
LEFT JOIN bl_3nf.ce_product_groups gr
ON mp.product_group_srcid = gr.product_group_srcid 
WHERE NOT EXISTS (
					SELECT DISTINCT 1
					FROM bl_3nf.ce_products t 
					WHERE t.product_srcid = mp.product_srcid 
						AND t.product_desc = mp.product_desc
						AND t.price = mp.price
						AND t.is_active = mp.is_active 
					)
ON CONFLICT ( product_srcid) DO  
UPDATE SET 
product_desc = EXCLUDED.product_desc,
product_group_id = EXCLUDED.product_group_id,
price = EXCLUDED.price,
is_active = EXCLUDED.is_active ,
update_dt = current_timestamp 
WHERE ce_products.product_desc != EXCLUDED.product_desc
	OR ce_products.product_group_id != EXCLUDED.product_group_id
	OR ce_products.price != EXCLUDED.price
	OR ce_products.is_active != EXCLUDED.is_active ; 

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_products' ,
								diag_row_count, 
								flag);
							
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_products', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
