-- Secret values and the grants that make them readable, for
-- [secrets].backend = "postgres". Values are encrypted by aenv-api before
-- they reach this table: the master key lives in a file the api half mounts
-- and is never written here, and a ciphertext row is never written into a
-- Kubernetes Secret. Neither half of that pair is useful without the other,
-- which is the whole reason they are stored apart.
CREATE TABLE IF NOT EXISTS secret_values (
    name           TEXT   NOT NULL,
    version        BIGINT NOT NULL,
    -- 'opaque' is one value, 'fields' a set of named scalars for a handler
    -- that authenticates to the upstream itself.
    kind           TEXT   NOT NULL,
    ciphertext     BYTEA  NOT NULL,
    -- Per-row, 12 bytes, never reused. AES-GCM repeats catastrophically.
    nonce          BYTEA  NOT NULL,
    -- Travels with the version, not with the ref row: the broker reads the
    -- pin from wherever it read the value.
    allowed_hosts  TEXT[] NOT NULL DEFAULT '{}',
    created_at_ms  BIGINT NOT NULL,
    PRIMARY KEY (name, version),
    CONSTRAINT secret_values_name_fk FOREIGN KEY (name)
        REFERENCES secret_refs(name) ON DELETE CASCADE,
    CONSTRAINT secret_values_kind CHECK (kind IN ('opaque', 'fields')),
    CONSTRAINT secret_values_version_positive CHECK (version > 0)
);

-- One row per execution, replaced wholesale and deleted by execution_id
-- alone. `sandbox_id` is checked on read and is not part of the key: a
-- revocation names the incarnation, and two incarnations never share an id.
CREATE TABLE IF NOT EXISTS secret_grants (
    execution_id   TEXT   PRIMARY KEY,
    sandbox_id     TEXT   NOT NULL,
    names          TEXT[] NOT NULL,
    granted_at_ms  BIGINT NOT NULL
);
