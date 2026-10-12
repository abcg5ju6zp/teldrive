-- +goose Up

-- Durable receipts of remote (Telegram) messages published for an upload part.
-- One row per lease attempt; the state machine maps every remote message to
-- one of three reconciliation classes:
--   pending               -> not yet published (row dropped once the lease dies)
--   published / deleting  -> published but not committed (safe to delete remotely)
--   adopted               -> committed together with the stored upload part
CREATE TYPE /* TEMPLATE: schema */upload_part_receipt_state AS ENUM (
    'pending', 'published', 'adopted', 'deleting'
);

CREATE TABLE /* TEMPLATE: schema */upload_part_receipts (
    upload_id        UUID NOT NULL REFERENCES /* TEMPLATE: schema */upload_sessions(id) ON DELETE CASCADE,
    part_no          INTEGER NOT NULL,
    lease_token      UUID NOT NULL,
    channel_id       BIGINT NOT NULL,
    plain_size       BIGINT NOT NULL,
    message_id       BIGINT,
    stored_size      BIGINT,
    checksum         TEXT,
    salt             TEXT,
    block_hashes     BYTEA,
    state            /* TEMPLATE: schema */upload_part_receipt_state NOT NULL DEFAULT 'pending',
    lease_expires_at TIMESTAMPTZ NOT NULL,
    published_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (upload_id, part_no, lease_token),
    CONSTRAINT upload_part_receipts_part_no_positive CHECK (part_no > 0),
    CONSTRAINT upload_part_receipts_plain_size_nonnegative CHECK (plain_size >= 0),
    CONSTRAINT upload_part_receipts_stored_size_nonnegative CHECK (stored_size IS NULL OR stored_size >= 0),
    CONSTRAINT upload_part_receipts_publication_check CHECK (
        (
            state = 'pending'
            AND message_id IS NULL
            AND stored_size IS NULL
            AND published_at IS NULL
        )
        OR
        (
            state IN ('published', 'adopted', 'deleting')
            AND message_id IS NOT NULL
            AND stored_size IS NOT NULL
            AND published_at IS NOT NULL
        )
    )
);

-- A published Telegram message backs at most one receipt.
CREATE UNIQUE INDEX upload_part_receipts_unique_message_idx
    ON /* TEMPLATE: schema */upload_part_receipts (channel_id, message_id)
    WHERE message_id IS NOT NULL;

-- Sweep of leases that died before a remote message was published.
CREATE INDEX upload_part_receipts_pending_expiry_idx
    ON /* TEMPLATE: schema */upload_part_receipts (lease_expires_at)
    WHERE state = 'pending';

-- Takeover and cleanup lookups for one part of a session.
CREATE INDEX upload_part_receipts_part_state_idx
    ON /* TEMPLATE: schema */upload_part_receipts (upload_id, part_no, state);

-- Sweeps of messages awaiting remote deletion.
CREATE INDEX upload_part_receipts_message_state_idx
    ON /* TEMPLATE: schema */upload_part_receipts (state, channel_id, message_id)
    WHERE message_id IS NOT NULL;

-- Backfill one receipt per existing upload part so the atomic StorePart CTE
-- never sees a legacy part without a receipt. Stored parts are already
-- committed; every other row becomes an already-dead pending receipt.
INSERT INTO /* TEMPLATE: schema */upload_part_receipts (
    upload_id,
    part_no,
    lease_token,
    channel_id,
    plain_size,
    message_id,
    stored_size,
    checksum,
    salt,
    block_hashes,
    state,
    lease_expires_at,
    published_at
)
SELECT
    up.upload_id,
    up.part_no,
    COALESCE(up.lease_token, gen_random_uuid()),
    up.channel_id,
    up.plain_size,
    CASE WHEN up.state = 'stored' THEN up.message_id ELSE NULL END,
    CASE WHEN up.state = 'stored' THEN up.stored_size ELSE NULL END,
    CASE WHEN up.state = 'stored' THEN up.checksum ELSE NULL END,
    CASE WHEN up.state = 'stored' THEN up.salt ELSE NULL END,
    CASE WHEN up.state = 'stored' THEN up.block_hashes ELSE NULL END,
    CASE WHEN up.state = 'stored' THEN 'adopted' ELSE 'pending' END
        ::/* TEMPLATE: schema */upload_part_receipt_state,
    COALESCE(up.lease_expires_at, now()),
    CASE WHEN up.state = 'stored' THEN now() ELSE NULL END
FROM /* TEMPLATE: schema */upload_parts up;

-- +goose Down

DROP TABLE IF EXISTS /* TEMPLATE: schema */upload_part_receipts;
DROP TYPE IF EXISTS /* TEMPLATE: schema */upload_part_receipt_state;
