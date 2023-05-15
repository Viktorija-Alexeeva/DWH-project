CREATE OR REPLACE PROCEDURE BL_CL.load_dim_shipments ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';

INSERT INTO bl_dm.dim_shipments  
SELECT CASE 
			WHEN s.shipment_id = -1 THEN -1
			ELSE nextval('BL_DM.dim_shipments_seq')
		END  AS shipment_surr_id ,
		COALESCE(s.shipment_id, -1) AS shipment_id , 
		COALESCE(s."mode", 'N/A') AS shipment_mode ,
		COALESCE(s."type", 'N/A') AS shipment_type ,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt	
FROM BL_3NF.ce_shipments s
WHERE NOT EXISTS 
				(
				SELECT DISTINCT 1
				FROM BL_DM.dim_shipments t 
				WHERE t.shipment_id = s.shipment_id 
					AND t.shipment_mode = s."mode"
					AND t.shipment_type = s."type"
				)
ON CONFLICT (shipment_id) DO UPDATE SET  
shipment_mode = EXCLUDED.shipment_mode,
shipment_type = EXCLUDED.shipment_type,
update_dt = current_timestamp 
WHERE dim_shipments.shipment_mode != EXCLUDED.shipment_mode
	OR dim_shipments.shipment_type != EXCLUDED.shipment_type ;


-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_DM',     -- INSERT ROW into table log_data if no errors
								'dim_shipments' ,
								diag_row_count, 
								flag);
							
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_DM',      -- INSERT ROW into table log_data if error
        					'dim_shipments', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
