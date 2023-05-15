
CREATE TABLE IF NOT EXISTS BL_CL.map_supplier_managers
				(
				manager_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				manager_name VARCHAR(50),
				manager_phone VARCHAR(15),
				manager_email VARCHAR(50),
				supplier_srcid VARCHAR(50),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);
 