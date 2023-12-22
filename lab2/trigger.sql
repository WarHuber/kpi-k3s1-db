CREATE OR REPLACE FUNCTION log_order_sum_trigger()
RETURNS TRIGGER AS $$
BEGIN
    -- Log the sum of the order
    RAISE NOTICE 'Order ID: %, Sum: %', NEW.id, NEW.sum;

    -- Check if the order sum is above a threshold
    IF NEW.sum > 1000 THEN
        RAISE NOTICE 'High-value order detected: %', NEW.id;
    END IF;

    -- Returning the record is necessary for AFTER triggers
    RETURN NEW;
EXCEPTION
    WHEN OTHERS THEN
        -- Handle exceptions
        RAISE NOTICE 'Error in log_order_sum_trigger: %', SQLERRM;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;