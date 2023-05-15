CREATE OR REPLACE PROCEDURE BL_CL.load_dim_markets  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
			
INSERT INTO bl_dm.dim_markets 
SELECT CASE 
			WHEN mar.market_id = -1 THEN -1
			ELSE nextval('BL_DM.dim_markets_seq') 
		END AS market_surr_id ,
		COALESCE(mar.market_id, -1) AS market_id , 
		COALESCE(mar.market_desc, 'N/A') AS market_desc,
		COALESCE(mar.address_id, -1) AS market_address_id ,
		COALESCE(ad.address_desc, 'N/A') AS market_address_desc ,
		COALESCE(ad.address_city_id, -1) AS market_address_city_id , 
		COALESCE(ci.address_city_desc, 'N/A') AS market_address_city_desc ,
		COALESCE(ci.address_city_country_id, -1) AS market_address_city_country_id , 
		COALESCE(cnt.address_city_country_desc, 'N/A') AS market_address_city_country_desc ,
		COALESCE(cnt.address_city_country_region_id, -1) AS market_address_city_country_region_id , 
		COALESCE(reg.address_city_country_region_desc, 'N/A') AS market_address_city_country_region_desc ,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt	
FROM BL_3NF.ce_markets mar
LEFT JOIN bl_3nf.ce_addresses ad
ON  mar.address_id  = ad.address_id  
LEFT JOIN bl_3nf.ce_address_cities ci
ON  ad.address_city_id  = ci.address_city_id 
LEFT JOIN bl_3nf.ce_address_city_countries cnt
ON  ci.address_city_country_id  = cnt.address_city_country_id 
LEFT JOIN bl_3nf.ce_address_city_country_regions reg
ON  cnt.address_city_country_region_id  = reg.address_city_country_region_id 
WHERE NOT EXISTS 
				(
				SELECT DISTINCT 1
				FROM bl_dm.dim_markets t 
				WHERE t.market_id = mar.market_id 
					AND t.market_desc = mar.market_desc 
					AND t.market_address_id = mar.address_id
				)
ON CONFLICT (market_id) DO UPDATE SET  
market_desc = EXCLUDED.market_desc,
market_address_id = EXCLUDED.market_address_id ,
market_address_desc = EXCLUDED.market_address_desc ,
market_address_city_id = EXCLUDED.market_address_city_id ,
market_address_city_desc = EXCLUDED.market_address_city_desc ,
market_address_city_country_id = EXCLUDED.market_address_city_country_id ,
market_address_city_country_desc = EXCLUDED.market_address_city_country_desc ,
market_address_city_country_region_id = EXCLUDED.market_address_city_country_region_id ,
market_address_city_country_region_desc = EXCLUDED.market_address_city_country_region_desc ,
update_dt = current_timestamp 
WHERE dim_markets.market_desc != EXCLUDED.market_desc
	OR dim_markets.market_address_id != EXCLUDED.market_address_id 
	OR dim_markets.market_address_desc != EXCLUDED.market_address_desc 
	OR dim_markets.market_address_city_id != EXCLUDED.market_address_city_id 
	OR dim_markets.market_address_city_desc != EXCLUDED.market_address_city_desc 
	OR dim_markets.market_address_city_country_id != EXCLUDED.market_address_city_country_id 
	OR dim_markets.market_address_city_country_desc != EXCLUDED.market_address_city_country_desc 
	OR dim_markets.market_address_city_country_region_id != EXCLUDED.market_address_city_country_region_id 
	OR dim_markets.market_address_city_country_region_desc != EXCLUDED.market_address_city_country_region_desc ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_DM',     -- INSERT ROW into table log_data if no errors
								'dim_markets' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_DM',      -- INSERT ROW into table log_data if error
        					'dim_markets', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
