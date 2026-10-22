CREATE FUNCTION slow_renewal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.owner IS NOT NULL AND NEW.owner = OLD.owner THEN
        PERFORM pg_sleep(0.7);
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER slow_renewal BEFORE UPDATE ON public.solo_leases
FOR EACH ROW EXECUTE FUNCTION slow_renewal();
