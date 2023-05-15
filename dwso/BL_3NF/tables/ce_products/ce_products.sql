CREATE TABLE IF NOT EXISTS BL_3NF.ce_products
				(
				product_id BIGINT PRIMARY KEY NOT NULL,
				product_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				product_desc VARCHAR(255),
				product_group_id BIGINT NOT NULL REFERENCES bl_3nf.ce_product_groups (product_group_id),
				price DECIMAL(10,2),
				is_active BOOLEAN ,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (product_srcid)
				);
