CREATE TABLE IF NOT EXISTS BL_CL.map_product_group_categories
				(
				product_group_category_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				product_group_category_desc VARCHAR(20) ,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);
				
