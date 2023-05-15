CREATE OR REPLACE PROCEDURE BL_CL.load_ce_address_city_countries ()
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
			
INSERT INTO bl_3nf.ce_address_city_countries 
SELECT nextval('BL_3NF.ce_address_city_countries_seq') AS address_city_country_id,
		COALESCE(scs.market_country_id, '-1') AS address_city_country_srcid ,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.market_country, 'N/A') AS address_city_country_desc ,
		COALESCE(reg.address_city_country_region_id, -1) AS address_city_country_region_id,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
LEFT JOIN bl_3nf.ce_address_city_country_regions reg
ON scs.market_region = reg.address_city_country_region_desc 
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_countries t
				WHERE scs.market_country = t.address_city_country_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_address_city_countries_seq') AS address_city_country_id,
		COALESCE(scs.customer_country_id, '-1') AS address_city_country_srcid ,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.customer_country, 'N/A') AS address_city_country_desc ,
		COALESCE(reg.address_city_country_region_id, -1) AS address_city_country_region_id,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
LEFT JOIN bl_3nf.ce_address_city_country_regions reg
ON scs.customer_region = reg.address_city_country_region_desc
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_countries t
				WHERE scs.customer_country = t.address_city_country_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_address_city_countries_seq') AS address_city_country_id,
		COALESCE(scs.supplier_country_id, '-1') AS address_city_country_srcid ,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.supplier_country, 'N/A') AS address_city_country_desc ,
		COALESCE(reg.address_city_country_region_id, -1) AS address_city_country_region_id,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
LEFT JOIN bl_3nf.ce_address_city_country_regions reg
ON scs.supplier_region = reg.address_city_country_region_desc		
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_countries t
				WHERE scs.supplier_country = t.address_city_country_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_address_city_countries_seq') AS address_city_country_id,
		COALESCE(sos.market_country_id, '-1') AS address_city_country_srcid ,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.market_country, 'N/A') AS address_city_country_desc,
		COALESCE(reg.address_city_country_region_id, -1) AS address_city_country_region_id,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos
LEFT JOIN bl_3nf.ce_address_city_country_regions reg
ON sos.market_region = reg.address_city_country_region_desc 
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_countries t
				WHERE sos.market_country = t.address_city_country_desc 
				)
		AND sos.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_address_city_countries_seq') AS address_city_country_id,
		COALESCE(sos.customer_country_id, '-1') AS address_city_country_srcid ,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.customer_country, 'N/A') AS address_city_country_desc,
		COALESCE(reg.address_city_country_region_id, -1) AS address_city_country_region_id,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos
LEFT JOIN bl_3nf.ce_address_city_country_regions reg
ON sos.customer_region = reg.address_city_country_region_desc
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_countries t
				WHERE sos.customer_country = t.address_city_country_desc 
				)
		AND sos.is_processed = 'N'
	UNION ALL 
SELECT nextval('BL_3NF.ce_address_city_countries_seq') AS address_city_country_id,
		COALESCE(sos.supplier_country_id, '-1') AS address_city_country_srcid ,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.supplier_country, 'N/A') AS address_city_country_desc,
		COALESCE(reg.address_city_country_region_id, -1) AS address_city_country_region_id,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos
LEFT JOIN bl_3nf.ce_address_city_country_regions reg
ON sos.supplier_region = reg.address_city_country_region_desc  
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_countries t
				WHERE sos.supplier_country = t.address_city_country_desc 
				)
		AND sos.is_processed = 'N'
ON CONFLICT (address_city_country_desc) DO NOTHING;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_address_city_countries' ,
								diag_row_count, 
								flag);
							
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_address_city_countries', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

