CREATE TABLE IF NOT EXISTS BL_DM.fct_sales 
				(
				supplier_surr_id BIGINT NOT NULL REFERENCES BL_DM.dim_suppliers (supplier_surr_id),
				customer_surr_id BIGINT NOT NULL REFERENCES BL_DM.dim_customers (customer_surr_id),
				shipment_surr_id BIGINT NOT NULL REFERENCES BL_DM.dim_shipments (shipment_surr_id),
				market_surr_id BIGINT NOT NULL REFERENCES BL_DM.dim_markets (market_surr_id),
				employee_surr_id BIGINT NOT NULL REFERENCES BL_DM.dim_employees_scd (employee_surr_id),
				product_surr_id BIGINT NOT NULL REFERENCES BL_DM.dim_products (product_surr_id),
				loyal_program_surr_id BIGINT NOT NULL REFERENCES BL_DM.dim_loyal_programs (loyal_program_surr_id),
				order_surr_id BIGINT NOT NULL REFERENCES BL_DM.dim_orders (order_surr_id),
				order_date DATE NOT NULL,
				shipping_date DATE NOT NULL,
				shipping_cost DECIMAL(10,2) ,
				quantity INT ,
				discount DECIMAL(5,3) ,
				sales_amount DECIMAL(10,2) ,
				profit DECIMAL(10,2) ,
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL
				) 
PARTITION BY RANGE(order_date) ;	
