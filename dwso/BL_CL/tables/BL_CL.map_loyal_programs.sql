CREATE TABLE IF NOT EXISTS BL_CL.map_loyal_programs 
				(
				loyal_program_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				loyal_program_desc VARCHAR(50),
				loyal_discount DECIMAL(5,3),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL  
				);