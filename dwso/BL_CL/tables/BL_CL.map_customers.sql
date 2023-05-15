CREATE TABLE IF NOT EXISTS BL_CL.map_customers
				(
				customer_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				"name" VARCHAR(50),
				date_of_birth DATE,
				gender VARCHAR(10),
				email VARCHAR(50),
				phone VARCHAR(15),
				address_desc VARCHAR(255),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);