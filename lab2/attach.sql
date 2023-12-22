CREATE TRIGGER after_order_update_insert
AFTER INSERT OR UPDATE ON tbl_order
FOR EACH ROW EXECUTE FUNCTION log_order_sum_trigger();