CREATE OR REPLACE PROCEDURE BL_CL.curr_month_refresh_fct_sales() 
LANGUAGE PLPGSQL AS 
$$
DECLARE 
START_DT DATE := DATE_TRUNC('MONTH', CURRENT_DATE)::DATE;
END_DT DATE :=  (DATE_TRUNC('MONTH', CURRENT_DATE) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')::DATE;  
PART_NAME TEXT := 'BL_DM.FCT_SALES_PARTITION_' || TO_CHAR(START_DT, 'YYYY_MM');
--right value in partition is not included, so we need to write 1st day of next month to take the value of last day of this month
END_DT_R DATE := (DATE_TRUNC('MONTH', CURRENT_DATE) + INTERVAL '1 MONTH')::DATE; 
diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;  
BEGIN  

   flag = 'I';	
  
   EXECUTE FORMAT (
   'ALTER TABLE BL_DM.FCT_SALES DETACH PARTITION %I; 
		 TRUNCATE %I; ', PART_NAME, PART_NAME);
    EXECUTE FORMAT ( 
    E'INSERT INTO %I  
	SELECT COALESCE(sup.supplier_surr_id, -1) AS supplier_surr_id,
			COALESCE(cu.customer_surr_id, -1) AS customer_surr_id,
			COALESCE(sh.shipment_surr_id, -1) AS shipment_surr_id,
			COALESCE(mar.market_surr_id, -1) AS market_surr_id,
			COALESCE(emp.employee_surr_id, -1) AS employee_surr_id,
			COALESCE(pr.product_surr_id, -1) AS product_surr_id,
			COALESCE(loy.loyal_program_surr_id, -1) AS loyal_program_surr_id,
			COALESCE(ord.order_surr_id, -1) AS order_surr_id,
			COALESCE(s.order_date, \'1900-01-01\') AS order_date,
			COALESCE(shce.shipping_date, \'1900-01-01\') AS shipping_date,
			shce.shipping_cost AS shipping_cost,
			s.quantity AS quantity,
			s.discount AS discount,
			s.sales_amount AS sales_amount,
			s.profit AS profit,
			current_timestamp AS insert_dt,
			current_timestamp AS update_dt	
	FROM bl_3nf.ce_sales s
	LEFT JOIN BL_DM.dim_suppliers sup
	ON s.supplier_id = sup.supplier_id  
	LEFT JOIN BL_DM.dim_customers cu
	ON s.customer_id = cu.customer_id 
	LEFT JOIN BL_DM.dim_shipments sh
	ON s.shipment_id = sh.shipment_id 
	LEFT JOIN bl_3nf.ce_shipments shce
	ON s.shipment_id = shce.shipment_id 
	LEFT JOIN BL_DM.dim_markets mar
	ON s.market_id = mar.market_id 
	LEFT JOIN BL_DM.dim_employees_scd emp
	ON s.employee_id = emp.employee_id 
	LEFT JOIN BL_DM.dim_products pr 
	ON s.product_id = pr.product_id 
	LEFT JOIN BL_DM.dim_loyal_programs loy
	ON s.loyal_program_id = loy.loyal_program_id 
	LEFT JOIN BL_DM.dim_orders ord
	ON s.order_id = ord.order_id  
	WHERE NOT EXISTS 
					(
					SELECT DISTINCT 1
					FROM BL_DM.fct_sales t 
					WHERE t.supplier_surr_id = sup.supplier_surr_id
						AND t.customer_surr_id = cu.customer_surr_id
						AND t.shipment_surr_id = sh.shipment_surr_id
						AND t.market_surr_id = mar.market_surr_id
						AND t.employee_surr_id = emp.employee_surr_id
						AND t.product_surr_id = pr.product_surr_id
						AND t.loyal_program_surr_id = loy.loyal_program_surr_id
						AND t.order_surr_id = ord.order_surr_id
						AND t.order_date = s.order_date
						AND t.shipping_date = shce.shipping_date
						AND t.shipping_cost = shce.shipping_cost
						AND t.quantity = s.quantity
						AND t.discount = s.discount
						AND t.sales_amount = s.sales_amount
						AND t.profit = s.profit
					)
	AND s.order_date BETWEEN (%L) AND (%L)
	ON CONFLICT DO NOTHING ;', PART_NAME, START_DT, END_DT );
		EXECUTE FORMAT(
		'ALTER TABLE BL_DM.FCT_SALES ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L);
		', PART_NAME,  START_DT, END_DT_R
   );
  

-- get diagnostics	
EXECUTE FORMAT('SELECT COUNT(*) FROM BL_DM.FCT_SALES WHERE ORDER_DATE BETWEEN (%L) AND (%L); ',
   		START_DT, END_DT ) INTO diag_row_count;
		CALL BL_CL.load_log_data('BL_DM',     -- INSERT ROW into table log_data if no errors
								'fct_sales' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_DM',      -- INSERT ROW into table log_data if error
        					'fct_sales', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 
