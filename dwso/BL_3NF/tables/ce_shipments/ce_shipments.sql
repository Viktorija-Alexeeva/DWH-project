CREATE TABLE IF NOT EXISTS BL_3NF.ce_shipments 
				(
				shipment_id BIGINT PRIMARY KEY NOT NULL,
				shipment_srcid VARCHAR(50) ,
				source_system VARCHAR(50) ,
				source_table VARCHAR(50) ,
				"mode" VARCHAR(20) ,
				"type" VARCHAR(20),
				shipping_date DATE NOT NULL,
				shipping_cost DECIMAL(10,2),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (shipment_srcid)
				);
