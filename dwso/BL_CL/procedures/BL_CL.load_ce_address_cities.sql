CREATE OR REPLACE PROCEDURE BL_CL.load_ce_address_cities ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert from src_consumer_segment and  src_office_segment
			
INSERT INTO bl_3nf.ce_address_cities 
SELECT nextval('BL_3NF.ce_address_cities_seq') AS address_city_id,
		COALESCE(scs.market_city_id, '-1') AS address_city_srcid ,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.market_city, 'N/A') AS address_city_desc ,
		COALESCE(cnt.address_city_country_id, -1) AS address_city_country_id,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
LEFT JOIN bl_3nf.ce_address_city_countries cnt
ON scs.market_country = cnt.address_city_country_desc 
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_cities t
				WHERE scs.market_city = t.address_city_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_address_cities_seq') AS address_city_id,
		COALESCE(scs.customer_city_id, '-1')  AS address_city_srcid ,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.customer_city, 'N/A') AS address_city_desc ,
		COALESCE(cnt.address_city_country_id, -1) AS address_city_country_id,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
LEFT JOIN bl_3nf.ce_address_city_countries cnt
ON scs.customer_country = cnt.address_city_country_desc 
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_cities t
				WHERE scs.customer_city = t.address_city_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_address_cities_seq') AS address_city_id,
		COALESCE(scs.supplier_city_id, '-1')  AS address_city_srcid ,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.supplier_city, 'N/A') AS address_city_desc ,
		COALESCE(cnt.address_city_country_id, -1) AS address_city_country_id,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
LEFT JOIN bl_3nf.ce_address_city_countries cnt
ON scs.supplier_country = cnt.address_city_country_desc  	
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_cities t
				WHERE scs.supplier_city = t.address_city_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL
SELECT nextval('BL_3NF.ce_address_cities_seq') AS address_city_id,
		COALESCE(sos.market_city_id, '-1') AS address_city_srcid ,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.market_city, 'N/A') AS address_city_desc ,
		COALESCE(cnt.address_city_country_id, -1) AS address_city_country_id,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos
LEFT JOIN bl_3nf.ce_address_city_countries cnt
ON sos.market_country = cnt.address_city_country_desc  
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_cities t
				WHERE sos.market_city = t.address_city_desc 
				)
		AND sos.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_address_cities_seq') AS address_city_id,
		COALESCE(sos.customer_city_id, '-1')  AS address_city_srcid ,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.customer_city, 'N/A') AS address_city_desc ,
		COALESCE(cnt.address_city_country_id, -1) AS address_city_country_id,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos
LEFT JOIN bl_3nf.ce_address_city_countries cnt
ON sos.customer_country = cnt.address_city_country_desc
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_cities t
				WHERE sos.customer_city = t.address_city_desc 
				)
		AND sos.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_address_cities_seq') AS address_city_id,
		COALESCE(sos.supplier_city_id,'-1')   AS address_city_srcid ,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.supplier_city, 'N/A') AS address_city_desc ,
		COALESCE(cnt.address_city_country_id, -1) AS address_city_country_id,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos
LEFT JOIN bl_3nf.ce_address_city_countries cnt
ON sos.supplier_country = cnt.address_city_country_desc 
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_cities t
				WHERE sos.supplier_city = t.address_city_desc 
				)
		AND sos.is_processed = 'N'
ON CONFLICT (address_city_desc) DO NOTHING;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_address_cities' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_address_cities', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

