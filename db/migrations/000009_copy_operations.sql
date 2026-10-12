-- +goose Up

-- Recovery state for a file-tree copy. A single stable operation identity
-- pins the source tree snapshot, destination, conflict policy and every
-- per-part Telegram receipt, so retries, request cancellation and process
-- restarts all continue from the same facts instead of selecting a new tree,
-- re-copying confirmed parts or deleting messages that are published.
CREATE TYPE /* TEMPLATE: schema */copy_operation_state AS ENUM (
    'parts_pending',
    'published',
    'aborted'
);

CREATE TABLE /* TEMPLATE: schema */copy_operations (
    id UUID PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES /* TEMPLATE: schema */users(user_id) ON DELETE CASCADE,
    source_root_id UUID NOT NULL,
    target_parent_id UUID,
    requested_name TEXT,
    conflict_policy /* TEMPLATE: schema */name_conflict_policy NOT NULL,
    -- Pinned tree snapshot: each node carries its source id, pre-assigned new
    -- id and catalog attributes. Recovery never re-reads the source tree, so a
    -- source purged while copying does not change what gets published.
    tree_snapshot JSONB NOT NULL,
    state /* TEMPLATE: schema */copy_operation_state NOT NULL DEFAULT 'parts_pending',
    result_root_id UUID,
    last_error TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    CONSTRAINT copy_operations_requested_name_check CHECK (
        requested_name IS NULL OR length(btrim(requested_name)) > 0
    ),
    CONSTRAINT copy_operations_published_check CHECK (
        (state = 'published' AND result_root_id IS NOT NULL AND published_at IS NOT NULL)
        OR
        (state <> 'published' AND result_root_id IS NULL AND published_at IS NULL)
    )
);

-- Recovery sweeps only scan stale, unfinished operations. updated_at is the
-- receipt heartbeat while Telegram parts are still being copied.
CREATE INDEX copy_operations_pending_idx
    ON /* TEMPLATE: schema */copy_operations (updated_at)
    WHERE state = 'parts_pending';

CREATE INDEX copy_operations_user_idx
    ON /* TEMPLATE: schema */copy_operations (user_id, created_at DESC);

CREATE TABLE /* TEMPLATE: schema */copy_operation_parts (
    operation_id UUID NOT NULL REFERENCES /* TEMPLATE: schema */copy_operations(id) ON DELETE CASCADE,
    source_file_id UUID NOT NULL,
    part_no INTEGER NOT NULL,
    source_channel_id BIGINT NOT NULL,
    source_message_id BIGINT NOT NULL,
    dest_channel_id BIGINT NOT NULL,
    -- A non-null destination message is a confirmed Telegram receipt. Null
    -- means the remote copy has never been confirmed and must be retried.
    dest_message_id BIGINT,
    plain_size BIGINT,
    stored_size BIGINT,
    checksum TEXT,
    salt TEXT,
    block_hashes BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    copied_at TIMESTAMPTZ,
    PRIMARY KEY (operation_id, source_file_id, part_no),
    CONSTRAINT copy_operation_parts_part_no_positive CHECK (part_no > 0),
    CONSTRAINT copy_operation_parts_source_message_positive CHECK (source_message_id > 0),
    CONSTRAINT copy_operation_parts_dest_message_positive CHECK (
        dest_message_id IS NULL OR dest_message_id > 0
    )
);

-- Compensation must not delete a message another live operation already holds
-- a receipt for (Telegram message ids can be reused after deletion).
CREATE INDEX copy_operation_parts_dest_idx
    ON /* TEMPLATE: schema */copy_operation_parts (dest_channel_id, dest_message_id)
    WHERE dest_message_id IS NOT NULL;

-- +goose Down

DROP TABLE IF EXISTS /* TEMPLATE: schema */copy_operation_parts;
DROP TABLE IF EXISTS /* TEMPLATE: schema */copy_operations;
DROP TYPE IF EXISTS /* TEMPLATE: schema */copy_operation_state;
