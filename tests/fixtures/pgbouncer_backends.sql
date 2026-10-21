CREATE TABLE observed_backends (pid integer NOT NULL);
CREATE FUNCTION observe_backend() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO observed_backends (pid) VALUES (pg_backend_pid());
    RETURN NEW;
END $$;
CREATE TRIGGER observe_backend BEFORE UPDATE ON public.solo_leases
FOR EACH ROW EXECUTE FUNCTION observe_backend();
