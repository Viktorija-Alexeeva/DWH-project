CREATE OR REPLACE PROCEDURE BL_CL.load_src_office_segment ()
LANGUAGE plpgsql
AS $$
DECLARE 
	diag_row_count INT;
    flag           VARCHAR(4);
    error_message  TEXT;
    error_context   TEXT;
BEGIN 
	flag = 'I';
	
INSERT INTO sa_office_segment.src_office_segment 
SELECT *
FROM sa_office_segment.ext_office_segment ecs
WHERE NOT EXISTS (
					SELECT 1 FROM sa_office_segment.src_office_segment sos
					WHERE ecs.order_srcid = sos.order_srcid 
						AND ecs.order_priority = sos.order_priority 
						AND ecs.order_date = sos.order_date
						AND ecs.ship_date = sos.ship_date
						AND ecs.shipment_srcid = sos.shipment_srcid
						AND ecs.ship_mode = sos.ship_mode
						AND ecs.shipment_type = sos.shipment_type
						AND ecs.customer_srcid = sos.customer_srcid
						AND ecs.customer_name = sos.customer_name 
						AND ecs.customer_dob = sos.customer_dob
						AND ecs.customer_full_age = sos.customer_full_age
						AND ecs.customer_gender = sos.customer_gender
						AND ecs.customer_email = sos.customer_email 
						AND ecs.customer_phone = sos.customer_phone
						AND ecs.customer_address_id = sos.customer_address_id
						AND ecs.customer_address = sos.customer_address
						AND ecs.customer_city_id = sos.customer_city_id
						AND ecs.customer_city = sos.customer_city
						AND ecs.customer_country_id = sos.customer_country_id
						AND ecs.customer_country = sos.customer_country
						AND ecs.customer_region_id = sos.customer_region_id 
						AND ecs.customer_region = sos.customer_region
						AND ecs.supplier_srcid = sos.supplier_srcid
						AND ecs.supplier_name = sos.supplier_name 
						AND ecs.manager_srcid = sos.manager_srcid 
						AND ecs.supplier_manager_name = sos.supplier_manager_name
						AND ecs.supplier_manager_phone = sos.supplier_manager_phone
						AND ecs.supplier_manager_email = sos.supplier_manager_email
						AND ecs.supplier_address_id = sos.supplier_address_id
						AND ecs.supplier_address = sos.supplier_address
						AND ecs.supplier_city_id = sos.supplier_city_id
						AND ecs.supplier_city = sos.supplier_city
						AND ecs.supplier_country_id = sos.supplier_country_id 
						AND ecs.supplier_country = sos.supplier_country
						AND ecs.supplier_region_id = sos.supplier_region_id
						AND ecs.supplier_region = sos.supplier_region
						AND ecs.supplier_status = sos.supplier_status
						AND ecs.market_srcid = sos.market_srcid
						AND ecs.market_desc = sos.market_desc
						AND ecs.market_address_id = sos.market_address_id
						AND ecs.market_address = sos.market_address
						AND ecs.market_city_id = sos.market_city_id
						AND ecs.market_city = sos.market_city
						AND ecs.market_country_id = sos.market_country_id
						AND ecs.market_country = sos.market_country
						AND ecs.market_region_id = sos.market_region_id
						AND ecs.market_region = sos.market_region
						AND ecs.employee_srcid = sos.employee_srcid
						AND ecs.employee_name = sos.employee_name
						AND ecs.employee_surname = sos.employee_surname
						AND ecs.employee_full_name = sos.employee_full_name
						AND ecs.employee_dob = sos.employee_dob
						AND ecs.employee_email = sos.employee_email
						AND ecs.employee_phone = sos.employee_phone
						AND ecs.employee_position = sos.employee_position
						AND ecs.product_srcid = sos.product_srcid
						AND ecs.product_desc = sos.product_desc
						AND ecs.product_category_id = sos.product_category_id
						AND ecs.product_category_desc = sos.product_category_desc
						AND ecs.product_group_id = sos.product_group_id
						AND ecs.product_group_desc = sos.product_group_desc
						AND ecs.product_price = sos.product_price
						AND ecs.sales_amount = sos.sales_amount
						AND ecs.quantity = sos.quantity
						AND ecs.discount = sos.discount
						AND ecs.profit = sos.profit
						AND ecs.shipping_cost = sos.shipping_cost
						AND ecs.product_status = sos.product_status
						AND ecs.loyal_program_srcid = sos.loyal_program_srcid
						AND ecs.loyal_program_desc = sos.loyal_program_desc
						AND ecs.loyal_discount	= sos.loyal_discount
					); 

-- get diagnostics	
	GET DIAGNOSTICS diag_row_count = ROW_COUNT;
		CALL BL_CL.load_log_data('BL_CL',     -- INSERT ROW into table log_data if no errors
								'src_office_segment' ,
								diag_row_count, 
								flag);
EXCEPTION
    WHEN OTHERS THEN
        flag = 'E';
        GET STACKED DIAGNOSTICS
            error_message = MESSAGE_TEXT,
            error_context = PG_CONTEXT;
        CALL bl_cl.load_log_data('BL_CL',      -- INSERT ROW into table log_data if error
        					'src_office_segment', 
        					diag_row_count,
                            flag,
                            error_message,
                            error_context);
		RAISE NOTICE 'Error message: %', error_message;
		RAISE NOTICE 'Error context: %', error_context;
       
COMMIT; 
END;
$$ 

