CREATE TABLE IF NOT EXISTS BL_CL.map_products
				(
				product_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				product_desc VARCHAR(255),
				product_group_srcid VARCHAR(50),
				price DECIMAL(10,2),
				is_active BOOLEAN ,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);
