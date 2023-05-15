CREATE TABLE IF NOT EXISTS BL_DM.dim_markets 
				(
				market_surr_id BIGINT PRIMARY KEY NOT NULL,
				market_id BIGINT UNIQUE NOT NULL REFERENCES BL_3NF.ce_markets(market_id), 
				market_desc VARCHAR (50)NOT NULL,
				market_address_id BIGINT NOT NULL REFERENCES BL_3NF.ce_addresses(address_id), 
				market_address_desc VARCHAR (255)NOT NULL,
				market_address_city_id BIGINT NOT NULL REFERENCES BL_3NF.ce_address_cities(address_city_id), 
				market_address_city_desc VARCHAR (50)NOT NULL,
				market_address_city_country_id BIGINT NOT NULL REFERENCES BL_3NF.ce_address_city_countries(address_city_country_id), 
				market_address_city_country_desc VARCHAR (50)NOT NULL,
				market_address_city_country_region_id BIGINT NOT NULL REFERENCES BL_3NF.ce_address_city_country_regions(address_city_country_region_id), 
				market_address_city_country_region_desc VARCHAR (20)NOT NULL,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				) ;