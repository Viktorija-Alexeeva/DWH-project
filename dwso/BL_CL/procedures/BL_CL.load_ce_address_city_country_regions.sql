CREATE OR REPLACE PROCEDURE BL_CL.load_ce_address_city_country_regions ()
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
			
INSERT INTO bl_3nf.ce_address_city_country_regions
SELECT nextval('BL_3NF.ce_address_city_country_regions_seq') AS address_city_country_region_id,
		COALESCE(scs.market_region_id, '-1') AS address_city_country_region_srcid,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.market_region, 'N/A') AS address_city_country_region_desc,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_country_regions t
				WHERE scs.market_region = t.address_city_country_region_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL
SELECT nextval('BL_3NF.ce_address_city_country_regions_seq') AS address_city_country_region_id,
		COALESCE(scs.customer_region_id, '-1') AS address_city_country_region_srcid,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.customer_region, 'N/A') AS address_city_country_region_desc,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_country_regions t
				WHERE scs.customer_region = t.address_city_country_region_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL
SELECT nextval('BL_3NF.ce_address_city_country_regions_seq') AS address_city_country_region_id,
		COALESCE(scs.supplier_region_id, '-1') AS address_city_country_region_srcid,
		'sa_consumer_segment', 
		'src_consumer_segment',
		COALESCE(scs.supplier_region, 'N/A') AS address_city_country_region_desc,
		current_timestamp ,
		current_timestamp 
FROM sa_consumer_segment.src_consumer_segment scs
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_country_regions t
				WHERE scs.supplier_region = t.address_city_country_region_desc 
				)
		AND scs.is_processed = 'N'
	UNION ALL
SELECT nextval('BL_3NF.ce_address_city_country_regions_seq') AS address_city_country_region_id,
		COALESCE(sos.market_region_id, '-1') AS address_city_country_region_srcid,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.market_region, 'N/A') AS address_city_country_region_desc,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_country_regions t
				WHERE sos.market_region = t.address_city_country_region_desc 
				)
		AND sos.is_processed = 'N'
	UNION ALL
SELECT nextval('BL_3NF.ce_address_city_country_regions_seq') AS address_city_country_region_id,
		COALESCE(sos.customer_region_id, '-1') AS address_city_country_region_srcid,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.customer_region, 'N/A') AS address_city_country_region_desc,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos 
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_country_regions t
				WHERE sos.customer_region = t.address_city_country_region_desc 
				)
		AND sos.is_processed = 'N'
	UNION ALL
SELECT nextval('BL_3NF.ce_address_city_country_regions_seq') AS address_city_country_region_id,
		COALESCE(sos.supplier_region_id, '-1') AS address_city_country_region_srcid,
		'sa_office_segment', 
		'src_office_segment',
		COALESCE(sos.supplier_region, 'N/A') AS address_city_country_region_desc,
		current_timestamp ,
		current_timestamp 
FROM sa_office_segment.src_office_segment sos	
WHERE NOT EXISTS (
				SELECT DISTINCT 1
				FROM bl_3nf.ce_address_city_country_regions t
				WHERE sos.supplier_region = t.address_city_country_region_desc 
				)
		AND sos.is_processed = 'N'
ON CONFLICT (address_city_country_region_desc) DO NOTHING ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_address_city_country_regions' ,
								diag_row_count, 
								flag);
							
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_address_city_country_regions', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

