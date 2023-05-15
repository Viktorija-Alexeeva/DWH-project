CREATE TABLE IF NOT EXISTS BL_DM.dim_employees_scd 
				(
				employee_surr_id BIGINT PRIMARY KEY NOT NULL,
				employee_id BIGINT NOT NULL , 
				employee_name VARCHAR (50)NOT NULL,
				employee_surname VARCHAR (50)NOT NULL,
				employee_full_name VARCHAR (100)NOT NULL,
				employee_date_of_birth DATE NOT NULL,
				employee_email VARCHAR (50)NOT NULL,
				employee_phone VARCHAR (15)NOT NULL,
				employee_position VARCHAR (50)NOT NULL,
				start_dt TIMESTAMP NOT NULL,
				end_dt TIMESTAMP NOT NULL,
				is_active BOOLEAN NOT NULL,
				insert_dt TIMESTAMP NOT NULL,
				CONSTRAINT dim_employees_ce_employees_fkey FOREIGN KEY (employee_id, start_dt) REFERENCES BL_3NF.ce_employees_scd(employee_id, start_dt)
				) ;
