CREATE OR REPLACE PROCEDURE BL_CL.load_ce_shipments  ()
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

WITH q AS (
			SELECT DISTINCT nextval('BL_3NF.ce_shipments_seq') AS shipment_id,
					COALESCE(scs.shipment_srcid, '-1') AS shipment_srcid ,
					'sa_consumer_segment' AS source_system , 
					'src_consumer_segment' AS source_table,
					COALESCE(scs.ship_mode, 'N/A') AS "mode",
					COALESCE(scs.shipment_type, 'N/A') AS "type",
					COALESCE(scs.ship_date :: DATE, '1900-01-01') AS shipping_date,
					COALESCE(scs.shipping_cost ::DECIMAL(10,2), -1) AS shipping_cost ,		
					current_timestamp AS insert_dt,
					current_timestamp AS update_dt 
			FROM sa_consumer_segment.src_consumer_segment scs
			WHERE scs.is_processed = 'N'
				UNION 
			SELECT DISTINCT nextval('BL_3NF.ce_shipments_seq') AS shipment_id,
					COALESCE(sos.shipment_srcid, '-1') AS shipment_srcid,
					'sa_office_segment' AS source_system, 
					'src_office_segment' AS source_table,
					COALESCE(sos.ship_mode, 'N/A') AS "mode",
					COALESCE(sos.shipment_type, 'N/A') AS "type",
					COALESCE(sos.ship_date :: DATE, '1900-01-01') AS shipping_date,
					COALESCE(sos.shipping_cost ::DECIMAL(10,2), -1) AS shipping_cost ,	
					current_timestamp AS insert_dt,
					current_timestamp AS update_dt 
			FROM sa_office_segment.src_office_segment sos
			WHERE sos.is_processed = 'N'
			)
MERGE INTO bl_3nf.ce_shipments sh  
USING q 
ON sh.shipment_srcid = q.shipment_srcid
WHEN MATCHED 
			AND sh."mode" != q."mode"
			OR 	sh."type" != q."type"
			OR 	sh.shipping_date != q.shipping_date
			OR 	sh.shipping_cost != q.shipping_cost
	THEN 
	UPDATE SET 
		"mode" = q."mode",
		"type" = q."type",
		shipping_date = q.shipping_date,
		shipping_cost = q.shipping_cost,
		update_dt = current_timestamp
WHEN NOT MATCHED THEN
	INSERT 
	VALUES (nextval('BL_3NF.ce_shipments_seq'),
			COALESCE(q.shipment_srcid, '-1'),
			q.source_system,
			q.source_table,
			COALESCE(q."mode", 'N/A'),
			COALESCE(q."type", 'N/A'),
			COALESCE(q.shipping_date, '1900-01-01'), 
			COALESCE(q.shipping_cost, -1),
			current_timestamp ,
			current_timestamp 
			) ;						

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_shipments' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_shipments', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
