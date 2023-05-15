
CREATE TABLE IF NOT EXISTS BL_3NF.ce_suppliers
				(
				supplier_id BIGINT PRIMARY KEY NOT NULL,
				supplier_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				"name" VARCHAR(50),
				address_id BIGINT NOT NULL REFERENCES bl_3nf.ce_addresses (address_id),
				is_active BOOLEAN,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (supplier_srcid)  
				);
				