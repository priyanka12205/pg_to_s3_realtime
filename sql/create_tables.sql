-- sql/create_tables.sql
-- Minimal retail-style table with an 'updated_at' column for CDC.
CREATE TABLE IF NOT EXISTS public.orders (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL,
    order_ts        TIMESTAMP NOT NULL DEFAULT NOW(), -- event time
    amount_usd      NUMERIC(12,2) NOT NULL,
    currency_code   TEXT NOT NULL DEFAULT 'USD',
    status          TEXT NOT NULL DEFAULT 'PLACED',
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Keep updated_at fresh on any update.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_updated_at ON public.orders;
CREATE TRIGGER trg_set_updated_at
BEFORE UPDATE ON public.orders
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();