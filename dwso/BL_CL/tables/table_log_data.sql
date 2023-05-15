-- create table log_data											
CREATE TABLE IF NOT EXISTS BL_CL.log_data (
							"session_user" TEXT NOT NULL DEFAULT current_user,
							log_time TIMESTAMP NOT NULL DEFAULT current_timestamp ,
							schema_name TEXT NOT NULL, 
							table_name TEXT NOT NULL,
							diag_row_count INT,
							flag VARCHAR(4), 
							error_message TEXT, 
							error_context TEXT
							);