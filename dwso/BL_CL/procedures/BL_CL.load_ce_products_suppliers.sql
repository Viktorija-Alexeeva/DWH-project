CREATE OR REPLACE PROCEDURE BL_CL.load_ce_products_suppliers  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
INSERT INTO bl_3nf.ce_products_suppliers 
SELECT COALESCE(pr.product_id, '-1') , 
		COALESCE(sup.supplier_id, '-1'),
		current_timestamp ,
		current_timestamp 
FROM bl_3nf.ce_products pr
CROSS JOIN bl_3nf.ce_suppliers sup
WHERE NOT EXISTS (
					SELECT 1 
					FROM  bl_3nf.ce_products_suppliers t
					WHERE t.product_id = pr.product_id 
						AND t.supplier_id = sup.supplier_id 
					)
ON CONFLICT DO NOTHING ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_products_suppliers' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_products_suppliers', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
