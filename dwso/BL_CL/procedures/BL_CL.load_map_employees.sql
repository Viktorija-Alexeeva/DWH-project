CREATE OR REPLACE PROCEDURE BL_CL.load_map_employees ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
-- insert from src_consumer_segment  and  src_office_segment		
		
INSERT INTO BL_CL.map_employees
SELECT DISTINCT employee_srcid, 'bl_cl' AS source_system, 'map_employees' AS source_table, "name" , surname, full_name, 
		date_of_birth, email, phone, market_srcid, "position", insert_dt , update_dt 
FROM 
	(
	SELECT DISTINCT ON (employee_srcid) scs.employee_srcid AS employee_srcid,
		scs.employee_name AS "name",
		scs.employee_surname AS surname,
		scs.employee_name || ' ' || scs.employee_surname AS full_name,
		scs.employee_dob ::DATE AS date_of_birth,
		scs.employee_email AS email,
		scs.employee_phone AS phone,
		scs.market_srcid AS market_srcid,
		scs.employee_position AS "position",
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt 
	FROM sa_consumer_segment.src_consumer_segment scs	
	WHERE NOT EXISTS (
					SELECT DISTINCT 1
					FROM BL_CL.map_employees mem 
					WHERE mem.full_name = scs.employee_full_name
					AND mem.date_of_birth::DATE = scs.employee_dob::DATE 
					AND mem.email = scs.employee_email
					AND mem.phone = scs.employee_phone
					AND mem.market_srcid = scs.market_srcid 
					AND mem."position" = scs.employee_position
					) 
			AND scs.is_processed = 'N'
		UNION ALL 
	SELECT DISTINCT ON (employee_srcid) sos.employee_srcid AS employee_srcid,
		sos.employee_name AS "name",
		sos.employee_surname AS surname,
		sos.employee_name || ' ' || sos.employee_surname AS full_name,
		sos.employee_dob ::DATE AS date_of_birth,
		sos.employee_email AS email,
		sos.employee_phone AS phone,
		sos.market_srcid AS market_srcid,
		sos.employee_position AS "position",
		current_timestamp AS insert_dt,
		current_timestamp AS update_dt 
	FROM sa_office_segment.src_office_segment  sos	
	WHERE NOT EXISTS (
					SELECT DISTINCT 1
					FROM BL_CL.map_employees mem 
					WHERE mem.full_name = sos.employee_full_name
					AND mem.date_of_birth::DATE = sos.employee_dob::DATE 
					AND mem.email = sos.employee_email
					AND mem.phone = sos.employee_phone
					AND mem.market_srcid = sos.market_srcid 
					AND mem."position" = sos.employee_position
					)
			AND sos.is_processed = 'N'
	) AS q
GROUP BY employee_srcid, source_system, source_table, "name" , surname, full_name, 
		date_of_birth, email, phone, market_srcid, "position",
		insert_dt , update_dt ;

				
				
				
-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_CL',     -- INSERT ROW into table log_data if no errors
								'map_employees' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'map_employees', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

