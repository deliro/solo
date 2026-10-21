CREATE SEQUENCE renewal_attempt;
CREATE TABLE renewal_fault (code text NOT NULL, failures bigint NOT NULL);
CREATE FUNCTION fail_renewal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    fault renewal_fault%ROWTYPE;
BEGIN
    IF NEW.owner IS NOT NULL AND NEW.owner = OLD.owner THEN
        SELECT code, failures INTO fault FROM renewal_fault;
        IF nextval('renewal_attempt') <= fault.failures THEN
            IF fault.code = 'timeout' THEN
                PERFORM pg_sleep(1);
            ELSE
                RAISE EXCEPTION 'injected renewal failure' USING ERRCODE = fault.code;
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER fail_renewal BEFORE UPDATE ON public.solo_leases
FOR EACH ROW EXECUTE FUNCTION fail_renewal();
