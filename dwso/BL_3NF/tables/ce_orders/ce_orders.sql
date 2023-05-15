CREATE TABLE IF NOT EXISTS BL_3NF.ce_orders 
				(
				order_id BIGINT PRIMARY KEY NOT NULL,
				order_srcid VARCHAR(50),
				source_system VARCHAR(50) ,
				source_table VARCHAR(50) ,
				order_priority VARCHAR(20) ,
				order_date DATE NOT NULL,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				UNIQUE (order_srcid )
				);