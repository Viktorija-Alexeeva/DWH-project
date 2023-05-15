CREATE OR REPLACE PROCEDURE BL_CL.load_ce_customers  ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert from map_customers
			
INSERT INTO bl_3nf.ce_customers 
SELECT DISTINCT nextval('BL_3NF.ce_customers_seq') AS customer_id,
		COALESCE(mcu.customer_srcid, '-1') AS customer_srcid ,
		mcu.source_system AS source_system ,
		mcu.source_table AS source_table ,
		COALESCE(mcu."name", 'N/A') AS "name",
		COALESCE(mcu.date_of_birth, '1900-01-01') AS date_of_birth,
		EXTRACT(YEAR FROM age(current_date, mcu.date_of_birth))::INT AS full_age,
		COALESCE(mcu.gender, 'N/A') AS gender,
		COALESCE(mcu.email, 'N/A') AS email ,
		COALESCE(mcu.phone, 'N/A') AS phone ,
		COALESCE(ad.address_id, -1) AS address_id,
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt 
FROM BL_CL.map_customers mcu
LEFT JOIN bl_3nf.ce_addresses ad
ON  mcu.address_desc = ad.address_desc  
WHERE NOT EXISTS (
					SELECT DISTINCT 1
					FROM bl_3nf.ce_customers t 
					WHERE t.customer_srcid = mcu.customer_srcid
						AND t."name" = mcu."name"
						AND t.date_of_birth = mcu.date_of_birth
						AND t.gender = mcu.gender 
						AND t.email = mcu.email 
						AND t.phone  = mcu.phone 
					)
ON CONFLICT (customer_srcid) DO UPDATE SET 
"name" = EXCLUDED."name" , 
date_of_birth = EXCLUDED.date_of_birth,
full_age = EXCLUDED.full_age,
gender = EXCLUDED.gender,
email = EXCLUDED.email,
phone = EXCLUDED.phone,
address_id = EXCLUDED.address_id,
update_dt = current_timestamp 
WHERE ce_customers."name" != EXCLUDED."name"
	OR ce_customers.date_of_birth != EXCLUDED.date_of_birth
	OR ce_customers.full_age != EXCLUDED.full_age
	OR ce_customers.gender != EXCLUDED.gender
	OR ce_customers.email != EXCLUDED.email
	OR ce_customers.phone != EXCLUDED.phone
	OR ce_customers.address_id != EXCLUDED.address_id ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_customers' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_customers', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 


