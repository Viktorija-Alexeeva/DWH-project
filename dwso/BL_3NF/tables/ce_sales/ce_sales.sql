
CREATE TABLE IF NOT EXISTS BL_3NF.ce_sales
				(
				supplier_id BIGINT NOT NULL REFERENCES bl_3nf.ce_suppliers (supplier_id),
				customer_id BIGINT NOT NULL REFERENCES bl_3nf.ce_customers (customer_id),
				shipment_id BIGINT NOT NULL REFERENCES bl_3nf.ce_shipments (shipment_id),
				market_id BIGINT NOT NULL REFERENCES bl_3nf.ce_markets (market_id),
				employee_id BIGINT NOT NULL ,
				product_id BIGINT NOT NULL REFERENCES bl_3nf.ce_products (product_id),
				loyal_program_id BIGINT NOT NULL REFERENCES bl_3nf.ce_loyal_programs (loyal_program_id),
				order_id BIGINT UNIQUE NOT NULL REFERENCES bl_3nf.ce_orders (order_id),
				order_date DATE NOT NULL ,
				quantity INT ,
				discount DECIMAL(5,3),
				sales_amount DECIMAL(10,2),
				profit DECIMAL(10,2), 
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL 
				);
