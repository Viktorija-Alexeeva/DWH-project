CREATE TABLE IF NOT EXISTS BL_CL.map_product_groups
				(
				product_group_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				product_group_desc VARCHAR(50),
				product_group_category_srcid VARCHAR(50), 
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				);
			