
CREATE TABLE IF NOT EXISTS BL_CL.map_markets
				(
				market_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				market_desc VARCHAR(50),
				address_desc VARCHAR(255),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL 
				);