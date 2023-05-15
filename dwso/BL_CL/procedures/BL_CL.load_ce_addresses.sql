CREATE OR REPLACE PROCEDURE BL_CL.load_ce_addresses  ()
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
			
INSERT INTO bl_3nf.ce_addresses 
SELECT nextval('BL_3NF.ce_addresses_seq') AS address_id,
		COALESCE(scs.market_address_id, '-1') AS address_srcid,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.market_address, 'N/A') AS address_desc,
		COALESCE(ci.address_city_id, -1) AS address_city_id,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
LEFT JOIN bl_3nf.ce_address_cities ci
ON scs.market_city = ci.address_city_desc 
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_addresses t
				WHERE scs.market_address = t.address_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_addresses_seq') AS address_id,
		COALESCE(scs.customer_address_id, '-1') AS address_srcid,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.customer_address, 'N/A') AS address_desc,
		COALESCE(ci.address_city_id, -1) AS address_city_id,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
LEFT JOIN bl_3nf.ce_address_cities ci
ON scs.customer_city = ci.address_city_desc  	
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_addresses t
				WHERE scs.customer_address = t.address_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_addresses_seq') AS address_id,
		COALESCE(scs.supplier_address_id, '-1') AS address_srcid,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.supplier_address, 'N/A') AS address_desc,
		COALESCE(ci.address_city_id, -1) AS address_city_id,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
LEFT JOIN bl_3nf.ce_address_cities ci
ON scs.supplier_city = ci.address_city_desc
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_addresses t
				WHERE scs.supplier_address = t.address_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_addresses_seq') AS address_id,
		COALESCE(sos.market_address_id, '-1') AS address_srcid,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.market_address, 'N/A') AS address_desc,
		COALESCE(ci.address_city_id, -1) AS address_city_id,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos
LEFT JOIN bl_3nf.ce_address_cities ci
ON sos.market_city = ci.address_city_desc
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_addresses t
				WHERE sos.market_address = t.address_desc 
				)
		AND sos.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_addresses_seq') AS address_id,
		COALESCE(sos.customer_address_id, '-1') AS address_srcid,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.customer_address, 'N/A') AS address_desc,
		COALESCE(ci.address_city_id, -1) AS address_city_id,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos
LEFT JOIN bl_3nf.ce_address_cities ci
ON sos.customer_city = ci.address_city_desc 
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_addresses t
				WHERE sos.customer_address = t.address_desc 
				)
		AND sos.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_addresses_seq') AS address_id,
		COALESCE(sos.supplier_address_id, '-1') AS address_srcid,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.supplier_address, 'N/A') AS address_desc,
		COALESCE(ci.address_city_id, -1) AS address_city_id,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos
LEFT JOIN bl_3nf.ce_address_cities ci
ON sos.supplier_city = ci.address_city_desc
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_addresses t
				WHERE sos.supplier_address = t.address_desc 
				)
		AND sos.is_processed = 'N'
ON CONFLICT (address_desc) DO NOTHING;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_addresses' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_addresses', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

