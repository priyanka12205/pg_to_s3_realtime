-- sql/seed_orders.sql
-- Load some sample rows and simulate a few updates.
INSERT INTO public.orders (customer_id, order_ts, amount_usd, currency_code, status)
VALUES
  (101, NOW() - INTERVAL '2 hours', 59.99, 'USD', 'PLACED'),
  (102, NOW() - INTERVAL '90 minutes', 129.50, 'USD', 'PLACED'),
  (103, NOW() - INTERVAL '70 minutes', 15.00, 'USD', 'PLACED');

-- Update one row to bump updated_at
UPDATE public.orders SET status = 'SHIPPED' WHERE id = 1;