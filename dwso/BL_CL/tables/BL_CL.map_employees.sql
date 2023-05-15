CREATE TABLE IF NOT EXISTS BL_CL.map_employees
				(
				employee_srcid VARCHAR(50) NOT NULL,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				"name" VARCHAR(50),
				surname VARCHAR(50),
				full_name VARCHAR(100),
				date_of_birth DATE,
				email VARCHAR(50),
				phone VARCHAR(15),
				market_srcid VARCHAR(50),
				"position" VARCHAR(50),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);
