CREATE OR REPLACE PROCEDURE BL_CL.load_map_customers ()
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

INSERT INTO BL_CL.map_customers
SELECT DISTINCT customer_srcid , 'bl_cl' AS source_system , 'map_customers' AS source_table ,
		"name" ,date_of_birth, gender, email , phone, address_desc , insert_dt , update_dt  
FROM 
	(				 
	SELECT DISTINCT scs.customer_srcid AS customer_srcid ,
			COALESCE(scs.customer_name, 'N/A') AS "name",
			COALESCE(scs.customer_dob ::DATE, '1900-01-01') AS date_of_birth,
			COALESCE(scs.customer_gender, 'N/A') AS gender,
			COALESCE(scs.customer_email, 'N/A') AS email ,
			COALESCE(scs.customer_phone, 'N/A') AS phone ,
			COALESCE(scs.customer_address, 'N/A') AS address_desc ,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_consumer_segment.src_consumer_segment scs
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_customers t 
						WHERE t."name" = scs.customer_name
							AND t.date_of_birth = scs.customer_dob::DATE
							AND t.gender = scs.customer_gender 
							AND t.email = scs.customer_email
							AND t.phone  = scs.customer_phone
							AND t.address_desc = scs.customer_address
						)
				AND scs.is_processed = 'N'
		UNION ALL 
	SELECT DISTINCT sos.customer_srcid AS customer_srcid ,
			COALESCE(sos.customer_name, 'N/A') AS "name",
			COALESCE(sos.customer_dob ::DATE, '1900-01-01') AS date_of_birth,
			COALESCE(sos.customer_gender, 'N/A') AS gender,
			COALESCE(sos.customer_email, 'N/A') AS email ,
			COALESCE(sos.customer_phone, 'N/A') AS phone ,
			COALESCE(sos.customer_address, 'N/A') AS address_desc ,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt 
	FROM sa_office_segment.src_office_segment sos
	WHERE NOT EXISTS (
						SELECT DISTINCT 1
						FROM BL_CL.map_customers t 
						WHERE t."name" = sos.customer_name
							AND t.date_of_birth = sos.customer_dob::DATE
							AND t.gender = sos.customer_gender 
							AND t.email = sos.customer_email
							AND t.phone  = sos.customer_phone
							AND t.address_desc = sos.customer_address
						)
				AND sos.is_processed = 'N'
	) AS q
GROUP BY customer_srcid , source_system , source_table , "name" , date_of_birth, 
		gender, email , phone, address_desc , insert_dt , update_dt  ;					
						
-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_CL',     -- INSERT ROW into table log_data if no errors
								'map_customers' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'map_customers', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 


