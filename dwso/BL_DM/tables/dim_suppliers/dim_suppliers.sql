CREATE TABLE IF NOT EXISTS BL_DM.dim_suppliers 
				(
				supplier_surr_id BIGINT PRIMARY KEY NOT NULL,
				supplier_id BIGINT UNIQUE NOT NULL REFERENCES BL_3NF.ce_suppliers(supplier_id), 
				supplier_name VARCHAR (50)NOT NULL,
				supplier_manager_id BIGINT NOT NULL REFERENCES BL_3NF.ce_supplier_managers(manager_id),
				supplier_manager_name VARCHAR (50)NOT NULL ,
				supplier_manager_phone VARCHAR (15)NOT NULL,
				supplier_manager_email VARCHAR (50)NOT NULL,
				supplier_address_id BIGINT NOT NULL REFERENCES BL_3NF.ce_addresses(address_id), 
				supplier_address_desc VARCHAR (255)NOT NULL,
				supplier_address_city_id BIGINT NOT NULL REFERENCES BL_3NF.ce_address_cities(address_city_id), 
				supplier_address_city_desc VARCHAR (50)NOT NULL,
				supplier_address_city_country_id BIGINT NOT NULL REFERENCES BL_3NF.ce_address_city_countries(address_city_country_id), 
				supplier_address_city_country_desc VARCHAR (50)NOT NULL,
				supplier_address_city_country_region_id BIGINT NOT NULL REFERENCES BL_3NF.ce_address_city_country_regions(address_city_country_region_id), 
				supplier_address_city_country_region_desc VARCHAR (20)NOT NULL,
				is_active BOOLEAN NOT NULL,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				) ;
				