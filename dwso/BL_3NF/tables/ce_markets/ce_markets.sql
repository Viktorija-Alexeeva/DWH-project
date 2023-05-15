
CREATE TABLE IF NOT EXISTS BL_3NF.ce_markets
				(
				market_id BIGINT PRIMARY KEY NOT NULL,
				market_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				market_desc VARCHAR(50),
				address_id BIGINT NOT NULL REFERENCES bl_3nf.ce_addresses (address_id),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (market_srcid)  
				);