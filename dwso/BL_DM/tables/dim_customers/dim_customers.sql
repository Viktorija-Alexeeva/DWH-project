CREATE TABLE IF NOT EXISTS BL_DM.dim_customers 
				(
				customer_surr_id BIGINT PRIMARY KEY NOT NULL,
				customer_id BIGINT UNIQUE NOT NULL REFERENCES BL_3NF.ce_customers(customer_id), 
				customer_name VARCHAR (50)NOT NULL,
				customer_date_of_birth DATE NOT NULL,
				customer_full_age INT NOT NULL,
				customer_gender VARCHAR (10)NOT NULL,
				customer_email VARCHAR (50)NOT NULL,
				customer_phone VARCHAR (15)NOT NULL,
				customer_address_id BIGINT NOT NULL REFERENCES BL_3NF.ce_addresses(address_id), 
				customer_address_desc VARCHAR (255)NOT NULL,
				customer_address_city_id BIGINT NOT NULL REFERENCES BL_3NF.ce_address_cities(address_city_id), 
				customer_address_city_desc VARCHAR (50)NOT NULL,
				customer_address_city_country_id BIGINT NOT NULL REFERENCES BL_3NF.ce_address_city_countries(address_city_country_id), 
				customer_address_city_country_desc VARCHAR (50)NOT NULL,
				customer_address_city_country_region_id BIGINT NOT NULL REFERENCES BL_3NF.ce_address_city_country_regions(address_city_country_region_id), 
				customer_address_city_country_region_desc VARCHAR (20)NOT NULL,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				) ;
				
