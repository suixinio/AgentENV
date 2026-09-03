-- Names and versions of the secrets the /secrets API manages. Values live in
-- the secrets store; this table never holds one.
CREATE TABLE IF NOT EXISTS secret_refs (
    secret_id       TEXT    PRIMARY KEY,
    name            TEXT    NOT NULL,
    current_version BIGINT  NOT NULL DEFAULT 0,
    metadata        JSONB   NOT NULL DEFAULT '{}'::jsonb,
    created_at_ms   BIGINT  NOT NULL,
    updated_at_ms   BIGINT  NOT NULL,
    CONSTRAINT secret_refs_name_unique UNIQUE (name),
    CONSTRAINT secret_refs_version_nonnegative CHECK (current_version >= 0)
);
