CREATE TABLE IF NOT EXISTS BL_3NF.ce_address_city_country_regions
				(
				address_city_country_region_id BIGINT PRIMARY KEY NOT NULL,
				address_city_country_region_srcid VARCHAR(50) ,
				source_system VARCHAR(50),
				source_table VARCHAR(50),
				address_city_country_region_desc VARCHAR(20),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (address_city_country_region_desc)
				);