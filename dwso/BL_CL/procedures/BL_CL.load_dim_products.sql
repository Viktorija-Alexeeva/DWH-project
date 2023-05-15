CREATE OR REPLACE PROCEDURE BL_CL.load_dim_products  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';

INSERT INTO bl_dm.dim_products 
SELECT CASE 
			WHEN p.product_id = -1 THEN -1
			ELSE nextval('BL_DM.dim_products_seq')
		END AS product_surr_id ,
		COALESCE(p.product_id , -1) AS product_id , 
		COALESCE(p.product_desc , 'N/A') AS product_desc ,
		COALESCE(p.product_group_id , -1) AS product_group_id ,
		COALESCE(gr.product_group_desc , 'N/A') AS product_group_desc ,
		COALESCE(gr.product_group_category_id , -1) AS product_group_category_id ,
		COALESCE(c.product_group_category_desc , 'N/A') AS product_group_category_desc,
		COALESCE(p.price , -1) AS product_price ,
		COALESCE(p.is_active, 'N') AS is_active ,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt	
FROM BL_3NF.ce_products p
LEFT JOIN bl_3nf.ce_product_groups gr
ON p.product_group_id = gr.product_group_id 
LEFT JOIN bl_3nf.ce_product_group_categories c
ON gr.product_group_category_id = c.product_group_category_id 
WHERE NOT EXISTS 
				(
				SELECT DISTINCT 1
				FROM BL_DM.dim_products t 
				WHERE t.product_id = p.product_id 
					AND t.product_desc = p.product_desc
					AND t.product_group_id = p.product_group_id
					AND t.product_group_desc = gr.product_group_desc
					AND t.product_group_category_id = gr.product_group_category_id 
					AND t.product_group_category_desc = c.product_group_category_desc 
					AND t.product_price = p.price 
					AND t.is_active = p.is_active
				)
ON CONFLICT (product_id) DO UPDATE SET  
product_desc = EXCLUDED.product_desc,
product_group_id = EXCLUDED.product_group_id,
product_group_desc = EXCLUDED.product_group_desc ,
product_group_category_id = EXCLUDED.product_group_category_id ,
product_group_category_desc = EXCLUDED.product_group_category_desc ,
product_price = EXCLUDED.product_price ,
is_active = EXCLUDED.is_active ,
update_dt = current_timestamp 
WHERE dim_products.product_desc != EXCLUDED.product_desc
	OR dim_products.product_group_id != EXCLUDED.product_group_id
	OR dim_products.product_group_desc != EXCLUDED.product_group_desc 
	OR dim_products.product_group_category_id != EXCLUDED.product_group_category_id 
	OR dim_products.product_group_category_desc != EXCLUDED.product_group_category_desc 
	OR dim_products.product_price != EXCLUDED.product_price 
	OR dim_products.is_active != EXCLUDED.is_active ;


-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_DM',     -- INSERT ROW into table log_data if no errors
								'dim_products' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_DM',      -- INSERT ROW into table log_data if error
        					'dim_products', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

