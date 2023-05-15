CREATE TABLE IF NOT EXISTS BL_3NF.ce_product_group_categories
				(
				product_group_category_id BIGINT PRIMARY KEY NOT NULL,
				product_group_category_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				product_group_category_desc VARCHAR(20),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (product_group_category_srcid)  
				);