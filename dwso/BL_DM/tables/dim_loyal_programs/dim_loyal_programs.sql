CREATE TABLE IF NOT EXISTS BL_DM.dim_loyal_programs
				(
				loyal_program_surr_id BIGINT PRIMARY KEY NOT NULL,
				loyal_program_id BIGINT UNIQUE NOT NULL REFERENCES BL_3NF.ce_loyal_programs(loyal_program_id), 
				loyal_program_desc VARCHAR(50)NOT NULL,
				loyal_discount DECIMAL(5,3)NOT NULL,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				) ;