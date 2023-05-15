CREATE TABLE IF NOT EXISTS BL_3NF.ce_products_suppliers
				(
				product_id BIGINT NOT NULL REFERENCES bl_3nf.ce_products (product_id),
				supplier_id BIGINT NOT NULL REFERENCES bl_3nf.ce_suppliers (supplier_id),
				insert_dt TIMESTAMP NOT NULL,
				update_dt TIMESTAMP NOT NULL,
				CONSTRAINT product_supplier_pk PRIMARY KEY (product_id, supplier_id)
				);