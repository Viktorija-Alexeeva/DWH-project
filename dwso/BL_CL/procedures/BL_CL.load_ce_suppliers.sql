CREATE OR REPLACE PROCEDURE BL_CL.load_ce_suppliers  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert from map_suppliers
			
INSERT INTO bl_3nf.ce_suppliers 
SELECT nextval('BL_3NF.ce_suppliers_seq') AS supplier_id,
		COALESCE(msu.supplier_srcid, '-1') AS supplier_srcid ,
		msu.source_system AS source_system,
		msu.source_table AS source_table,
		COALESCE(msu."name", 'N/A') AS "name",
		COALESCE(ad.address_id, -1) AS address_id,
		COALESCE(msu.is_active, 'Y') AS is_active,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt  
FROM bl_cl.map_suppliers msu
LEFT JOIN bl_3nf.ce_addresses ad
ON msu.address_desc = ad.address_desc	
WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM bl_3nf.ce_suppliers t 
						WHERE t.supplier_srcid = msu.supplier_srcid
							AND t."name" = msu."name" 
							AND t.is_active  = msu.is_active 
						)
ON CONFLICT (supplier_srcid) DO UPDATE SET 
"name" = EXCLUDED."name",
address_id = EXCLUDED.address_id,
is_active = EXCLUDED.is_active,
update_dt = current_timestamp 
WHERE ce_suppliers."name" != EXCLUDED."name"
	OR ce_suppliers.address_id != EXCLUDED.address_id
	OR ce_suppliers.is_active != EXCLUDED.is_active ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_suppliers' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_suppliers', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

