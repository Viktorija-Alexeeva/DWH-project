CREATE TABLE IF NOT EXISTS BL_DM.dim_products
				(
				product_surr_id BIGINT PRIMARY KEY NOT NULL,
				product_id BIGINT UNIQUE NOT NULL REFERENCES BL_3NF.ce_products(product_id), 
				product_desc VARCHAR(255)NOT NULL,
				product_group_id BIGINT NOT NULL REFERENCES bl_3nf.ce_product_groups (product_group_id),
				product_group_desc VARCHAR(50)NOT NULL,
				product_group_category_id BIGINT NOT NULL REFERENCES bl_3nf.ce_product_group_categories (product_group_category_id),
				product_group_category_desc VARCHAR(20)NOT NULL,
				product_price DECIMAL(10,2)NOT NULL,
				is_active BOOLEAN NOT NULL,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				) ;
