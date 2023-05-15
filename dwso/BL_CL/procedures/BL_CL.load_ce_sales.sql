CREATE OR REPLACE PROCEDURE BL_CL.load_ce_sales ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
	diag_row_count1 INT;
	diag_row_count2 INT;
BEGIN 
	
-- insert from src_consumer_segment and src_office_segment

	flag = 'I';	
 	
WITH q AS (
			SELECT DISTINCT  COALESCE(sup.supplier_id, -1) AS supplier_id,
					COALESCE(cu.customer_id, -1) AS customer_id,
					COALESCE(sh.shipment_id, -1) AS shipment_id,
					COALESCE(mar.market_id, -1) AS market_id,
					COALESCE(emp.employee_id, -1) AS employee_id,
					COALESCE(pr.product_id, -1) AS product_id,
					COALESCE(loy.loyal_program_id, -1) AS loyal_program_id,
					COALESCE(ord.order_id, -1) AS order_id,
					COALESCE(ord.order_date, '1900-01-01') AS order_date,
					scs.quantity ::INT AS quantity ,
					CASE 
						WHEN loy.loyal_program_srcid = '0' THEN scs.discount ::DECIMAL(5,3)
						ELSE loy.loyal_discount 
					END AS discount, 
					scs.sales_amount:: DECIMAL(10,2) AS sales_amount ,
					scs.profit:: DECIMAL(10,2)AS profit,
					current_timestamp AS insert_dt ,
					current_timestamp AS update_dt 
			FROM sa_consumer_segment.src_consumer_segment scs
			LEFT JOIN bl_3nf.ce_orders ord
			ON scs.order_srcid = ord.order_srcid 
			LEFT JOIN bl_3nf.ce_loyal_programs loy
			ON scs.loyal_program_srcid = loy.loyal_program_srcid
			LEFT JOIN bl_3nf.ce_products pr 
			ON scs.product_srcid = pr.product_srcid
			LEFT JOIN bl_3nf.ce_employees_scd emp 
			ON scs.employee_srcid = emp.employee_srcid
			LEFT JOIN bl_3nf.ce_markets mar 
			ON scs.market_srcid = mar.market_srcid
			LEFT JOIN bl_3nf.ce_shipments sh 
			ON scs.shipment_srcid = sh.shipment_srcid
			LEFT JOIN bl_3nf.ce_customers cu
			ON scs.customer_srcid = cu.customer_srcid
			LEFT JOIN bl_3nf.ce_suppliers sup
			ON scs.supplier_srcid = sup.supplier_srcid 
			WHERE ord.order_date >= emp.start_dt::DATE AND ord.order_date < emp.end_dt::DATE 
				AND  scs.is_processed = 'N'
					UNION 
			SELECT DISTINCT COALESCE(sup.supplier_id,  -1) AS supplier_id,
					COALESCE(cu.customer_id, -1) AS customer_id,
					COALESCE(sh.shipment_id, -1) AS shipment_id,
					COALESCE(mar.market_id, -1) AS market_id,
					COALESCE(emp.employee_id, -1) AS employee_id,
					COALESCE(pr.product_id, -1) AS product_id,
					COALESCE(loy.loyal_program_id, -1) AS loyal_program_id,
					COALESCE(ord.order_id, -1) AS order_id,
					COALESCE(ord.order_date, '1900-01-01') AS order_date,
					sos.quantity ::INT AS quantity ,
					CASE 
						WHEN loy.loyal_program_srcid = '0' THEN sos.discount ::DECIMAL(5,3)
						ELSE loy.loyal_discount 
					END AS discount, 
					sos.sales_amount:: DECIMAL(10,2) AS sales_amount ,
					sos.profit:: DECIMAL(10,2) AS profit,
					current_timestamp AS insert_dt ,
					current_timestamp AS update_dt 
			FROM sa_office_segment.src_office_segment sos
			LEFT JOIN bl_3nf.ce_orders ord
			ON sos.order_srcid = ord.order_srcid 
			LEFT JOIN bl_3nf.ce_loyal_programs loy
			ON sos.loyal_program_srcid = loy.loyal_program_srcid
			LEFT JOIN bl_3nf.ce_products pr 
			ON sos.product_srcid = pr.product_srcid
			LEFT JOIN bl_3nf.ce_employees_scd emp 
			ON sos.employee_srcid = emp.employee_srcid
			LEFT JOIN bl_3nf.ce_markets mar 
			ON sos.market_srcid = mar.market_srcid
			LEFT JOIN bl_3nf.ce_shipments sh 
			ON sos.shipment_srcid = sh.shipment_srcid
			LEFT JOIN bl_3nf.ce_customers cu
			ON sos.customer_srcid = cu.customer_srcid
			LEFT JOIN bl_3nf.ce_suppliers sup
			ON sos.supplier_srcid = sup.supplier_srcid 
			WHERE ord.order_date >= emp.start_dt::DATE AND ord.order_date < emp.end_dt::DATE 
				AND sos.is_processed = 'N'
			)
MERGE INTO bl_3nf.ce_sales 
USING q 
ON ce_sales.order_id = q.order_id
WHEN MATCHED 
			AND ce_sales.supplier_id  != q.supplier_id
			OR ce_sales.customer_id != q.customer_id 
			OR ce_sales.shipment_id != q.shipment_id 
			OR ce_sales.market_id != q.market_id 
			OR ce_sales.employee_id != q.employee_id 
			OR ce_sales.product_id != q.product_id 
			OR ce_sales.loyal_program_id != q.loyal_program_id 
			OR ce_sales.order_date != q.order_date 
			OR ce_sales.quantity != q.quantity 
			OR ce_sales.discount != q.discount 
			OR ce_sales.sales_amount != q.sales_amount 
			OR ce_sales.profit != q.profit 
	THEN 
	UPDATE SET 
		supplier_id  = q.supplier_id,
		customer_id = q.customer_id ,
		shipment_id = q.shipment_id ,
		market_id = q.market_id ,
		employee_id = q.employee_id ,
		product_id = q.product_id ,
		loyal_program_id = q.loyal_program_id ,
		order_date = q.order_date ,
		quantity = q.quantity ,
		discount = q.discount ,
		sales_amount = q.sales_amount ,
		profit = q.profit,
		update_dt = current_timestamp
WHEN NOT MATCHED THEN
	INSERT 
	VALUES (
			q.supplier_id,
			q.customer_id ,
			q.shipment_id ,
			q.market_id ,
			q.employee_id ,
			q.product_id ,
			q.loyal_program_id,
			q.order_id,
			q.order_date ,
			q.quantity  ,
			q.discount ,
			q.sales_amount ,
			q.profit ,
			current_timestamp ,
			current_timestamp 
			) ;

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_3NF',     -- INSERT ROW into table log_data if no errors
								'ce_sales' ,
								diag_row_count, 
								flag);

EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_3NF',      -- INSERT ROW into table log_data if error
        					'ce_sales', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

