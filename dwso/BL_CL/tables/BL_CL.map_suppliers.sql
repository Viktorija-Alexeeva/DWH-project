
CREATE TABLE IF NOT EXISTS BL_CL.map_suppliers
				(
				supplier_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				"name" VARCHAR(50),
				address_desc VARCHAR(255),
				is_active BOOLEAN,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);