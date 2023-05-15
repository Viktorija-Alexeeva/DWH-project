CREATE TABLE IF NOT EXISTS BL_3NF.ce_employees_scd
				(
				employee_id BIGINT NOT NULL,
				employee_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				"name" VARCHAR(50),
				surname VARCHAR(50),
				full_name VARCHAR(100),
				date_of_birth DATE,
				email VARCHAR(50),
				phone VARCHAR(15),
				market_id BIGINT NOT NULL REFERENCES bl_3nf.ce_markets (market_id),
				"position" VARCHAR(50),
				start_dt TIMESTAMP NOT NULL,
				end_dt TIMESTAMP NOT NULL ,
				is_active BOOLEAN,
				insert_dt TIMESTAMP NOT NULL,
				CONSTRAINT employee_id_start_dt_pk PRIMARY KEY (employee_id, start_dt) 
				);
		
