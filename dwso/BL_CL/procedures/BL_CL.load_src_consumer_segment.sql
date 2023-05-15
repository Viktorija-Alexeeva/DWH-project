CREATE OR REPLACE PROCEDURE BL_CL.load_src_consumer_segment ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
INSERT INTO sa_consumer_segment.src_consumer_segment
SELECT *
FROM sa_consumer_segment.ext_consumer_segment ecs
WHERE NOT EXISTS (
					SELECT 1 FROM sa_consumer_segment.src_consumer_segment scs
					WHERE ecs.order_srcid = scs.order_srcid 
						AND ecs.order_priority = scs.order_priority 
						AND ecs.order_date = scs.order_date
						AND ecs.ship_date = scs.ship_date
						AND ecs.shipment_srcid = scs.shipment_srcid
						AND ecs.ship_mode = scs.ship_mode
						AND ecs.shipment_type = scs.shipment_type
						AND ecs.customer_srcid = scs.customer_srcid
						AND ecs.customer_name = scs.customer_name 
						AND ecs.customer_dob = scs.customer_dob
						AND ecs.customer_full_age = scs.customer_full_age
						AND ecs.customer_gender = scs.customer_gender
						AND ecs.customer_email = scs.customer_email 
						AND ecs.customer_phone = scs.customer_phone
						AND ecs.customer_address_id = scs.customer_address_id
						AND ecs.customer_address = scs.customer_address
						AND ecs.customer_city_id = scs.customer_city_id
						AND ecs.customer_city = scs.customer_city
						AND ecs.customer_country_id = scs.customer_country_id
						AND ecs.customer_country = scs.customer_country
						AND ecs.customer_region_id = scs.customer_region_id 
						AND ecs.customer_region = scs.customer_region
						AND ecs.supplier_srcid = scs.supplier_srcid
						AND ecs.supplier_name = scs.supplier_name 
						AND ecs.manager_srcid = scs.manager_srcid 
						AND ecs.supplier_manager_name = scs.supplier_manager_name
						AND ecs.supplier_manager_phone = scs.supplier_manager_phone
						AND ecs.supplier_manager_email = scs.supplier_manager_email
						AND ecs.supplier_address_id = scs.supplier_address_id
						AND ecs.supplier_address = scs.supplier_address
						AND ecs.supplier_city_id = scs.supplier_city_id
						AND ecs.supplier_city = scs.supplier_city
						AND ecs.supplier_country_id = scs.supplier_country_id 
						AND ecs.supplier_country = scs.supplier_country
						AND ecs.supplier_region_id = scs.supplier_region_id
						AND ecs.supplier_region = scs.supplier_region
						AND ecs.supplier_status = scs.supplier_status
						AND ecs.market_srcid = scs.market_srcid
						AND ecs.market_desc = scs.market_desc
						AND ecs.market_address_id = scs.market_address_id
						AND ecs.market_address = scs.market_address
						AND ecs.market_city_id = scs.market_city_id
						AND ecs.market_city = scs.market_city
						AND ecs.market_country_id = scs.market_country_id
						AND ecs.market_country = scs.market_country
						AND ecs.market_region_id = scs.market_region_id
						AND ecs.market_region = scs.market_region
						AND ecs.employee_srcid = scs.employee_srcid
						AND ecs.employee_name = scs.employee_name
						AND ecs.employee_surname = scs.employee_surname
						AND ecs.employee_full_name = scs.employee_full_name
						AND ecs.employee_dob = scs.employee_dob
						AND ecs.employee_email = scs.employee_email
						AND ecs.employee_phone = scs.employee_phone
						AND ecs.employee_position = scs.employee_position
						AND ecs.product_srcid = scs.product_srcid
						AND ecs.product_desc = scs.product_desc
						AND ecs.product_category_id = scs.product_category_id
						AND ecs.product_category_desc = scs.product_category_desc
						AND ecs.product_group_id = scs.product_group_id
						AND ecs.product_group_desc = scs.product_group_desc
						AND ecs.product_price = scs.product_price
						AND ecs.sales_amount = scs.sales_amount
						AND ecs.quantity = scs.quantity
						AND ecs.discount = scs.discount
						AND ecs.profit = scs.profit
						AND ecs.shipping_cost = scs.shipping_cost
						AND ecs.product_status = scs.product_status
						AND ecs.loyal_program_srcid = scs.loyal_program_srcid
						AND ecs.loyal_program_desc = scs.loyal_program_desc
						AND ecs.loyal_discount	= scs.loyal_discount
					); 

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_CL',     -- INSERT ROW into table log_data if no errors
								'src_consumer_segment' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'src_consumer_segment', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

