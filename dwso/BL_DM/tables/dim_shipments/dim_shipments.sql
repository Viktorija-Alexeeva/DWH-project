CREATE TABLE IF NOT EXISTS BL_DM.dim_shipments 
				(
				shipment_surr_id BIGINT PRIMARY KEY NOT NULL,
				shipment_id BIGINT UNIQUE NOT NULL REFERENCES BL_3NF.ce_shipments(shipment_id), 
				shipment_mode VARCHAR (20)NOT NULL,
				shipment_type VARCHAR (20)NOT NULL,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				) ;