CREATE TABLE public.solo_schema_version (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    version BIGINT NOT NULL CHECK (version > 0)
);
INSERT INTO public.solo_schema_version (singleton, version) VALUES (TRUE, 1);
CREATE TABLE public.solo_leases (
    key TEXT PRIMARY KEY,
    owner UUID,
    scheduled_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    last_success TIMESTAMPTZ,
    acquired_at TIMESTAMPTZ,
    owner_host TEXT,
    owner_pid BIGINT,
    last_completed_at TIMESTAMPTZ,
    last_outcome TEXT CHECK (last_outcome IN ('success', 'failure')),
    last_success_at TIMESTAMPTZ,
    CONSTRAINT solo_leases_ownership CHECK (
        (owner IS NULL AND scheduled_at IS NULL AND expires_at IS NULL)
        OR (owner IS NOT NULL AND scheduled_at IS NOT NULL AND expires_at IS NOT NULL)
    )
);
CREATE TABLE public.solo_successes (
    key TEXT NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (key, scheduled_at)
);
