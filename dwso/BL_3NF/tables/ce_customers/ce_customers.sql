CREATE TABLE IF NOT EXISTS BL_3NF.ce_customers
				(
				customer_id BIGINT PRIMARY KEY NOT NULL,
				customer_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				"name" VARCHAR(50),
				date_of_birth DATE,
				full_age INT ,  
				gender VARCHAR(10),
				email VARCHAR(50),
				phone VARCHAR(15),
				address_id BIGINT NOT NULL REFERENCES bl_3nf.ce_addresses (address_id),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (customer_srcid)  
				);