CREATE OR REPLACE PROCEDURE BL_CL.load_dim_orders  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
				
INSERT INTO bl_dm.dim_orders  
SELECT CASE 
			WHEN o.order_id = -1 THEN -1
			ELSE nextval('BL_DM.dim_orders_seq')
		END  AS order_surr_id ,
		COALESCE(o.order_id, -1) AS order_id , 
		COALESCE(o.order_priority, 'N/A') AS order_priority ,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt	
FROM BL_3NF.ce_orders o
WHERE NOT EXISTS 
				(
				SELECT DISTINCT 1
				FROM bl_dm.dim_orders t 
				WHERE t.order_id = o.order_id 
					AND t.order_priority = o.order_priority
				)
ON CONFLICT (order_id) DO UPDATE SET  
order_priority = EXCLUDED.order_priority,
update_dt = current_timestamp 
WHERE dim_orders.order_priority != EXCLUDED.order_priority ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_DM',     -- INSERT ROW into table log_data if no errors
								'dim_orders' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_DM',      -- INSERT ROW into table log_data if error
        					'dim_orders', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
