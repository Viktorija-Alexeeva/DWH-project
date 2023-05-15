-- proc to load log_data
CREATE OR REPLACE PROCEDURE BL_CL.load_log_data (i_schema_name TEXT, 
											i_table_name TEXT, 
											i_diag_row_count INT,
											i_flag VARCHAR(4), 
											i_error_message TEXT DEFAULT NULL, 
											i_error_context TEXT DEFAULT NULL) 
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO BL_CL.log_data(schema_name, table_name, diag_row_count, flag, error_message, error_context)
	SELECT 	i_schema_name AS schema_name, 
			i_table_name AS table_name, 
			i_diag_row_count AS diag_row_count ,
			i_flag AS flag, 
			i_error_message AS error_message,
			i_error_context AS error_context;
END;
$$ 					