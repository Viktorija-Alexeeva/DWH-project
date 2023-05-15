CREATE TABLE IF NOT EXISTS BL_3NF.ce_loyal_programs 
				(
				loyal_program_id BIGINT PRIMARY KEY NOT NULL,
				loyal_program_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				loyal_program_desc VARCHAR(50),
				loyal_discount DECIMAL(5,3),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (loyal_program_srcid)  
				);