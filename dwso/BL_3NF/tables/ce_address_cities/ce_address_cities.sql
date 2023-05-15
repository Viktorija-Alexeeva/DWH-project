
CREATE TABLE IF NOT EXISTS BL_3NF.ce_address_cities
				(
				address_city_id BIGINT PRIMARY KEY NOT NULL,
				address_city_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				address_city_desc VARCHAR(50),
				address_city_country_id BIGINT NOT NULL REFERENCES bl_3nf.ce_address_city_countries (address_city_country_id),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (address_city_desc)  
				);