CREATE TABLE IF NOT EXISTS BL_DM.dim_orders 
				(
				order_surr_id BIGINT PRIMARY KEY NOT NULL,
				order_id BIGINT UNIQUE NOT NULL REFERENCES BL_3NF.ce_orders(order_id), 
				order_priority VARCHAR (20)NOT NULL,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				) ;