
CREATE TABLE IF NOT EXISTS BL_3NF.ce_supplier_managers
				(
				manager_id BIGINT PRIMARY KEY NOT NULL,
				manager_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				manager_name VARCHAR(50),
				manager_phone VARCHAR(15),
				manager_email VARCHAR(50),
				supplier_id BIGINT NOT NULL REFERENCES bl_3nf.ce_suppliers (supplier_id),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (manager_srcid)  
				);
	