-- name: CreateUploadSession :one
INSERT INTO /* TEMPLATE: schema */upload_sessions (
    id,
    user_id,
    parent_id,
    name,
    expected_size,
    expected_hash_algorithm,
    expected_hash_value,
    mime_type,
    mod_time,
    encryption,
    encryption_key_version,
    conflict_policy,
    part_size,
    state,
    expires_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(user_id),
    sqlc.narg(parent_id),
    sqlc.arg(name),
    sqlc.arg(expected_size),
    sqlc.narg(expected_hash_algorithm),
    sqlc.narg(expected_hash_value),
    sqlc.narg(mime_type),
    sqlc.arg(mod_time),
    sqlc.arg(encryption),
    sqlc.narg(encryption_key_version),
    sqlc.arg(conflict_policy),
    sqlc.arg(part_size),
    'open',
    sqlc.arg(expires_at)
)
RETURNING *;

-- name: GetUploadSessionForUser :one
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id);


-- name: GetUploadSessionAnyOwner :one
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE id = sqlc.arg(upload_id);

-- name: ListUploadSessions :many
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE user_id = sqlc.arg(user_id)
  AND (sqlc.narg(state)::/* TEMPLATE: schema */upload_state IS NULL OR state = sqlc.narg(state)::/* TEMPLATE: schema */upload_state)
  AND (
    sqlc.narg(after_created_at)::timestamptz IS NULL
    OR (created_at, id) < (
      sqlc.narg(after_created_at)::timestamptz,
      sqlc.narg(after_id)::uuid
    )
  )
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_size);

-- name: FindResumableUploadSessions :many
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE user_id = sqlc.arg(user_id)
  AND parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid
  AND name = sqlc.arg(name)
  AND expected_size = sqlc.arg(expected_size)
  AND encryption = sqlc.arg(encryption)
  AND conflict_policy = 'replace'
  AND expires_at > now()
  AND state = 'open'
  AND mime_type IS NOT DISTINCT FROM sqlc.narg(mime_type)::text
  AND (
    NOT sqlc.arg(has_mod_time)::boolean
    OR abs(extract(epoch FROM (mod_time - sqlc.arg(mod_time)::timestamptz))) <= 1
  )
ORDER BY created_at DESC, id DESC;

-- name: GetUploadPart :one
SELECT *
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no);

-- name: ClaimUploadPart :one
INSERT INTO /* TEMPLATE: schema */upload_parts (
    upload_id,
    part_no,
    channel_id,
    plain_size,
    checksum,
    state,
    lease_token,
    lease_expires_at
) VALUES (
    sqlc.arg(upload_id),
    sqlc.arg(part_no),
    sqlc.arg(channel_id),
    sqlc.arg(plain_size),
    sqlc.narg(checksum),
    'uploading',
    sqlc.arg(lease_token),
    sqlc.arg(lease_expires_at)
)
ON CONFLICT (upload_id, part_no) DO UPDATE
SET channel_id = EXCLUDED.channel_id,
    plain_size = EXCLUDED.plain_size,
    checksum = EXCLUDED.checksum,
    state = 'uploading',
    lease_token = EXCLUDED.lease_token,
    lease_expires_at = EXCLUDED.lease_expires_at,
    last_error_code = NULL,
    updated_at = now()
WHERE /* TEMPLATE: schema */upload_parts.state <> 'stored'
  AND (
    /* TEMPLATE: schema */upload_parts.lease_expires_at IS NULL
    OR /* TEMPLATE: schema */upload_parts.lease_expires_at < now()
  )
RETURNING *;

-- name: RenewUploadPartLease :execrows
WITH receipt AS (
    UPDATE /* TEMPLATE: schema */upload_part_receipts
    SET lease_expires_at = sqlc.arg(lease_expires_at),
        updated_at = now()
    WHERE upload_id = sqlc.arg(upload_id)
      AND part_no = sqlc.arg(part_no)
      AND lease_token = sqlc.arg(lease_token)
)
UPDATE /* TEMPLATE: schema */upload_parts
SET lease_expires_at = sqlc.arg(lease_expires_at),
    updated_at = now()
WHERE upload_parts.upload_id = sqlc.arg(upload_id)
  AND upload_parts.part_no = sqlc.arg(part_no)
  AND upload_parts.state = 'uploading'
  AND upload_parts.lease_token = sqlc.arg(lease_token);

-- name: MarkUploadPartFailed :one
WITH drop_pending AS (
    DELETE FROM /* TEMPLATE: schema */upload_part_receipts
    WHERE upload_id = sqlc.arg(upload_id)
      AND part_no = sqlc.arg(part_no)
      AND lease_token = sqlc.arg(lease_token)
      AND state = 'pending'
)
UPDATE /* TEMPLATE: schema */upload_parts
SET state = 'failed',
    lease_token = NULL,
    lease_expires_at = NULL,
    last_error_code = sqlc.arg(error_code),
    updated_at = now()
WHERE upload_parts.upload_id = sqlc.arg(upload_id)
  AND upload_parts.part_no = sqlc.arg(part_no)
  AND upload_parts.lease_token = sqlc.arg(lease_token)
RETURNING upload_parts.*;

-- name: AdoptStoredPart :one
-- Commits the stored part and its remote-message receipt in one transaction.
-- The receipt may still be 'pending' for direct StorePart callers; after a
-- normal publish it is 'published'. The receipt owns the encryption salt once
-- a message has been published.
WITH target_receipt AS (
    SELECT target.*
    FROM /* TEMPLATE: schema */upload_part_receipts target
    WHERE target.upload_id = sqlc.arg(upload_id)
      AND target.part_no = sqlc.arg(part_no)
      AND target.lease_token = sqlc.arg(lease_token)
      AND target.state IN ('pending', 'published')
    FOR UPDATE OF target
),
stored AS (
    UPDATE /* TEMPLATE: schema */upload_parts p
    SET message_id = sqlc.arg(message_id),
        stored_size = sqlc.arg(stored_size),
        checksum = sqlc.narg(checksum),
        salt = COALESCE(tr.salt, sqlc.narg(salt)),
        block_hashes = sqlc.narg(block_hashes),
        state = 'stored',
        lease_token = NULL,
        lease_expires_at = NULL,
        last_error_code = NULL,
        updated_at = now()
    FROM target_receipt tr
    WHERE p.upload_id = tr.upload_id
      AND p.part_no = tr.part_no
      AND p.state = 'uploading'
      AND p.lease_token = tr.lease_token
      AND p.channel_id = tr.channel_id
    RETURNING p.*
),
adopted AS (
    UPDATE /* TEMPLATE: schema */upload_part_receipts r
    SET message_id = s.message_id,
        stored_size = s.stored_size,
        checksum = s.checksum,
        salt = COALESCE(r.salt, s.salt),
        block_hashes = s.block_hashes,
        state = 'adopted',
        published_at = COALESCE(r.published_at, now()),
        updated_at = now()
    FROM target_receipt tr
    CROSS JOIN stored s
    WHERE r.upload_id = tr.upload_id
      AND r.part_no = tr.part_no
      AND r.lease_token = tr.lease_token
)
SELECT stored.*
FROM stored;

-- name: ListUploadParts :many
SELECT *
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = sqlc.arg(upload_id)
  AND (
    sqlc.narg(after_part_no)::integer IS NULL
    OR part_no > sqlc.narg(after_part_no)::integer
  )
ORDER BY part_no
LIMIT sqlc.arg(page_size);

-- name: ListUploadPartsByUploadIDs :many
SELECT *
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = ANY(sqlc.arg(upload_ids)::uuid[])
ORDER BY upload_id, part_no;

-- name: LockUploadSessionForCompletion :one
SELECT *
FROM /* TEMPLATE: schema */upload_sessions
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id)
FOR UPDATE;

-- name: MarkUploadCompleting :one
UPDATE /* TEMPLATE: schema */upload_sessions
SET state = 'completing',
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id)
  AND state = 'open'
RETURNING *;

-- name: FinalizeUploadExpectedSize :one
UPDATE /* TEMPLATE: schema */upload_sessions
SET expected_size = sqlc.arg(expected_size),
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id)
  AND expected_size = -1
  AND state = 'open'
RETURNING *;

-- name: GetStoredUploadPartSummary :one
SELECT
    count(*)::integer AS part_count,
    COALESCE(sum(plain_size), 0)::bigint AS plain_size,
    COALESCE(sum(stored_size), 0)::bigint AS stored_size,
    COALESCE(min(part_no), 0)::integer AS min_part_no,
    COALESCE(max(part_no), 0)::integer AS max_part_no
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = sqlc.arg(upload_id)
  AND state = 'stored';

-- name: ListStoredUploadPartHashes :many
SELECT block_hashes
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = sqlc.arg(upload_id)
  AND state = 'stored'
ORDER BY part_no;

-- name: InsertFileFromUpload :one
INSERT INTO /* TEMPLATE: schema */files (
    id,
    user_id,
    parent_id,
    name,
    kind,
    mime_type,
    size,
    hash_algorithm,
    hash_value,
    encryption,
    encryption_key_version,
    status,
    mod_time
)
SELECT
    sqlc.arg(file_id),
    user_id,
    parent_id,
    name,
    'file',
    mime_type,
    expected_size,
    sqlc.narg(hash_algorithm),
    sqlc.narg(hash_value),
    encryption,
    encryption_key_version,
    'active',
    mod_time
FROM /* TEMPLATE: schema */upload_sessions us
WHERE us.id = sqlc.arg(upload_id)
  AND us.user_id = sqlc.arg(user_id)
  AND us.state = 'completing'
RETURNING *;

-- name: InsertFilePartsFromUpload :execrows
INSERT INTO /* TEMPLATE: schema */file_parts (
    file_id,
    part_no,
    channel_id,
    message_id,
    plain_size,
    stored_size,
    checksum,
    salt,
    block_hashes
)
SELECT
    sqlc.arg(file_id),
    part_no,
    channel_id,
    message_id,
    plain_size,
    stored_size,
    checksum,
    salt,
    block_hashes
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = sqlc.arg(upload_id)
  AND state = 'stored'
ORDER BY part_no;

-- name: CompleteUploadSession :one
UPDATE /* TEMPLATE: schema */upload_sessions
SET state = 'completed',
    file_id = sqlc.arg(file_id),
    completed_at = now(),
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id)
  AND state = 'completing'
RETURNING *;

-- name: AbortUploadSession :one
UPDATE /* TEMPLATE: schema */upload_sessions
SET state = 'aborted',
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id)
  AND state IN ('open', 'completing')
RETURNING *;

-- name: ExpireUploadSessions :many
UPDATE /* TEMPLATE: schema */upload_sessions
SET state = 'expired',
    updated_at = now()
WHERE id IN (
    SELECT id
    FROM /* TEMPLATE: schema */upload_sessions
    WHERE state IN ('open', 'completing')
      AND expires_at <= now()
    ORDER BY expires_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1000
)
RETURNING *;

-- name: ListUploadPartsForCleanupMany :many
SELECT up.*
FROM /* TEMPLATE: schema */upload_parts AS up
JOIN /* TEMPLATE: schema */upload_sessions AS us ON us.id = up.upload_id
WHERE us.id = ANY(sqlc.arg(upload_ids)::uuid[])
  AND us.state IN ('aborted', 'expired')
  AND up.message_id IS NOT NULL
ORDER BY up.upload_id, up.channel_id, up.part_no;

-- name: ListUploadSessionsPendingCleanup :many
SELECT DISTINCT us.*
FROM /* TEMPLATE: schema */upload_sessions us
WHERE us.state IN ('aborted', 'expired')
  AND (
      EXISTS (
          SELECT 1
          FROM /* TEMPLATE: schema */upload_parts up
          WHERE up.upload_id = us.id
            AND up.message_id IS NOT NULL
      )
      OR EXISTS (
          SELECT 1
          FROM /* TEMPLATE: schema */upload_part_receipts r
          WHERE r.upload_id = us.id
            AND r.state IN ('published', 'deleting')
            AND r.message_id IS NOT NULL
      )
  )
ORDER BY us.updated_at, us.id
LIMIT 1000;

-- name: DeleteUploadPartsForCleanup :execrows
DELETE FROM /* TEMPLATE: schema */upload_parts AS part
USING /* TEMPLATE: schema */upload_sessions AS session,
      jsonb_to_recordset(sqlc.arg(parts)::jsonb) AS cleanup_part(
        upload_id uuid, part_no integer, message_id bigint
      )
WHERE part.upload_id = cleanup_part.upload_id
  AND part.part_no = cleanup_part.part_no
  AND part.message_id = cleanup_part.message_id
  AND session.id = part.upload_id
  AND session.state IN ('aborted', 'expired');

-- name: LockUploadDestinationConflict :one
SELECT *
FROM /* TEMPLATE: schema */files
WHERE user_id = sqlc.arg(user_id)
  AND parent_id IS NOT DISTINCT FROM sqlc.narg(parent_id)::uuid
  AND name = sqlc.arg(name)
  AND status = 'active'
FOR UPDATE;

-- name: MarkActiveFileDeletionPendingForReplace :execrows
UPDATE /* TEMPLATE: schema */files
SET status = 'deletion_pending',
    deleted_at = COALESCE(deleted_at, now()),
    updated_at = now(),
    generation = generation + 1
WHERE id = sqlc.arg(file_id)
  AND user_id = sqlc.arg(user_id)
  AND status = 'active';

-- name: RevokeActiveSharesForFile :exec
UPDATE /* TEMPLATE: schema */file_shares
SET revoked_at = COALESCE(revoked_at, now())
WHERE file_id = sqlc.arg(file_id)
  AND owner_id = sqlc.arg(user_id)
  AND revoked_at IS NULL;

-- name: RenameUploadSession :execrows
UPDATE /* TEMPLATE: schema */upload_sessions
SET name = sqlc.arg(name),
    updated_at = now()
WHERE id = sqlc.arg(upload_id)
  AND user_id = sqlc.arg(user_id);

-- name: GetAllUploadPartSummary :one
SELECT
    count(*)::bigint AS total_parts,
    count(*) FILTER (WHERE state = 'stored')::bigint AS stored_parts,
    COALESCE(sum(plain_size) FILTER (WHERE state = 'stored'), 0)::bigint AS stored_plain_size,
    COALESCE(min(part_no) FILTER (WHERE state = 'stored'), 0)::integer AS min_part_no,
    COALESCE(max(part_no) FILTER (WHERE state = 'stored'), 0)::integer AS max_part_no
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = sqlc.arg(upload_id);

-- name: CountInvalidOpenEndedUploadParts :one
WITH final_part AS (
    SELECT COALESCE(max(part_no), 0)::integer AS part_no
    FROM /* TEMPLATE: schema */upload_parts
    WHERE upload_id = sqlc.arg(upload_id)
      AND state = 'stored'
)
SELECT count(*)::bigint
FROM /* TEMPLATE: schema */upload_parts parts
CROSS JOIN final_part
WHERE parts.upload_id = sqlc.arg(upload_id)
  AND parts.state = 'stored'
  AND parts.part_no < final_part.part_no
  AND parts.plain_size <> sqlc.arg(part_size);

-- name: ListUploadDailyStatistics :many
WITH days AS (
  SELECT generate_series(
    (CURRENT_DATE - (sqlc.arg(days)::integer - 1))::date,
    CURRENT_DATE,
    interval '1 day'
  )::date AS day
), totals AS (
  SELECT completed_at::date AS day,
         COALESCE(sum(expected_size), 0)::bigint AS uploaded_bytes,
         count(*)::bigint AS completed_files
  FROM /* TEMPLATE: schema */upload_sessions
  WHERE user_id = sqlc.arg(user_id)
    AND state = 'completed'
    AND completed_at >= CURRENT_DATE - (sqlc.arg(days)::integer - 1)
  GROUP BY completed_at::date
)
SELECT d.day, COALESCE(t.uploaded_bytes, 0)::bigint AS uploaded_bytes,
       COALESCE(t.completed_files, 0)::bigint AS completed_files
FROM days d
LEFT JOIN totals t USING (day)
ORDER BY d.day;

-- --------------------------------------------------------------------------
-- Remote message receipts: durable record of Telegram messages published for
-- an upload attempt, enabling publish-once retries and three-class cleanup
-- (not published / committed / safe to delete).
-- --------------------------------------------------------------------------

-- name: InsertPartReceipt :exec
INSERT INTO /* TEMPLATE: schema */upload_part_receipts (
    upload_id,
    part_no,
    lease_token,
    channel_id,
    plain_size,
    state,
    lease_expires_at
) VALUES (
    sqlc.arg(upload_id),
    sqlc.arg(part_no),
    sqlc.arg(lease_token),
    sqlc.arg(channel_id),
    sqlc.arg(plain_size),
    'pending',
    sqlc.arg(lease_expires_at)
);

-- name: LockUploadPart :one
SELECT *
FROM /* TEMPLATE: schema */upload_parts
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no)
FOR UPDATE;

-- name: FindTakeablePartReceipt :one
SELECT r.*
FROM /* TEMPLATE: schema */upload_part_receipts r
JOIN /* TEMPLATE: schema */upload_sessions us ON us.id = r.upload_id
JOIN /* TEMPLATE: schema */upload_parts p ON p.upload_id = r.upload_id AND p.part_no = r.part_no
WHERE r.upload_id = sqlc.arg(upload_id)
  AND r.part_no = sqlc.arg(part_no)
  AND us.user_id = sqlc.arg(user_id)
  AND r.plain_size = sqlc.arg(plain_size)
  AND us.state = 'open'
  AND us.expires_at > now()
  AND p.state <> 'stored'
  AND r.state = 'published'
  AND r.message_id IS NOT NULL
  AND (
      r.lease_token IS DISTINCT FROM p.lease_token
      OR r.lease_expires_at < now()
  )
ORDER BY r.published_at DESC, r.created_at DESC
LIMIT 1;

-- name: LockTakeablePartReceipt :one
SELECT r.*
FROM /* TEMPLATE: schema */upload_part_receipts r
JOIN /* TEMPLATE: schema */upload_sessions us ON us.id = r.upload_id
JOIN /* TEMPLATE: schema */upload_parts p ON p.upload_id = r.upload_id AND p.part_no = r.part_no
WHERE r.upload_id = sqlc.arg(upload_id)
  AND r.part_no = sqlc.arg(part_no)
  AND us.state = 'open'
  AND us.expires_at > now()
  AND p.state <> 'stored'
  AND r.state = 'published'
  AND r.message_id IS NOT NULL
  AND (
      r.lease_token IS DISTINCT FROM p.lease_token
      OR r.lease_expires_at < now()
  )
ORDER BY r.published_at DESC, r.created_at DESC
LIMIT 1
FOR UPDATE OF r;

-- name: ClaimUploadPartOnReceipt :one
UPDATE /* TEMPLATE: schema */upload_parts
SET channel_id = sqlc.arg(channel_id),
    plain_size = sqlc.arg(plain_size),
    checksum = sqlc.narg(checksum),
    state = 'uploading',
    lease_token = sqlc.arg(lease_token),
    lease_expires_at = sqlc.arg(lease_expires_at),
    last_error_code = NULL,
    updated_at = now()
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no)
  AND state <> 'stored'
  AND (
      lease_expires_at IS NULL
      OR lease_expires_at < now()
  )
RETURNING *;

-- name: InsertTakeoverReceipt :exec
INSERT INTO /* TEMPLATE: schema */upload_part_receipts (
    upload_id,
    part_no,
    lease_token,
    channel_id,
    plain_size,
    message_id,
    stored_size,
    salt,
    state,
    lease_expires_at,
    published_at
) VALUES (
    sqlc.arg(upload_id),
    sqlc.arg(part_no),
    sqlc.arg(lease_token),
    sqlc.arg(channel_id),
    sqlc.arg(plain_size),
    sqlc.arg(message_id),
    sqlc.arg(stored_size),
    sqlc.narg(salt),
    'published',
    sqlc.arg(lease_expires_at),
    sqlc.arg(published_at)
);

-- name: DeletePartReceiptByLease :exec
DELETE FROM /* TEMPLATE: schema */upload_part_receipts
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no)
  AND lease_token = sqlc.arg(lease_token);

-- name: RecordPartPublication :one
UPDATE /* TEMPLATE: schema */upload_part_receipts
SET message_id = sqlc.arg(message_id),
    stored_size = sqlc.arg(stored_size),
    salt = sqlc.narg(salt),
    state = 'published',
    published_at = now(),
    updated_at = now()
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no)
  AND lease_token = sqlc.arg(lease_token)
  AND state = 'pending'
RETURNING *;

-- name: CondemnPartReceipt :execrows
UPDATE /* TEMPLATE: schema */upload_part_receipts
SET state = 'deleting',
    updated_at = now()
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no)
  AND lease_token = sqlc.arg(lease_token)
  AND state = 'published';

-- name: AttachPartReceiptHashes :execrows
UPDATE /* TEMPLATE: schema */upload_part_receipts
SET checksum = sqlc.arg(checksum),
    block_hashes = sqlc.arg(block_hashes),
    updated_at = now()
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no)
  AND lease_token = sqlc.arg(lease_token)
  AND state = 'published';

-- name: DeletePartReceipt :exec
DELETE FROM /* TEMPLATE: schema */upload_part_receipts
WHERE upload_id = sqlc.arg(upload_id)
  AND part_no = sqlc.arg(part_no)
  AND lease_token = sqlc.arg(lease_token)
  AND message_id IS NOT DISTINCT FROM sqlc.narg(message_id);

-- Not-yet-published class: drop intent rows whose lease is dead. No remote
-- message exists for these receipts, so no Telegram call is involved.
-- name: DeleteDeadPendingReceipts :execrows
WITH dead AS (
    SELECT r.upload_id AS upload_id, r.part_no AS part_no, r.lease_token AS lease_token
    FROM /* TEMPLATE: schema */upload_part_receipts r
    JOIN /* TEMPLATE: schema */upload_sessions us ON us.id = r.upload_id
    WHERE r.state = 'pending'
      AND (
          r.lease_expires_at < now()
          OR us.state IN ('aborted', 'expired')
      )
    LIMIT 1000
)
DELETE FROM /* TEMPLATE: schema */upload_part_receipts
WHERE (upload_id, part_no, lease_token) IN (SELECT upload_id, part_no, lease_token FROM dead);

-- name: ListUploadPartReceiptsForCleanupMany :many
-- Safe-to-delete receipts of aborted or expired sessions whose message is not
-- referenced by any stored upload part or committed file part.
SELECT us.user_id AS user_id, r.*
FROM /* TEMPLATE: schema */upload_part_receipts r
JOIN /* TEMPLATE: schema */upload_sessions us ON us.id = r.upload_id
WHERE us.id = ANY(sqlc.arg(upload_ids)::uuid[])
  AND us.state IN ('aborted', 'expired')
  AND r.state IN ('published', 'deleting')
  AND r.message_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */upload_parts up
      WHERE up.channel_id = r.channel_id
        AND up.message_id = r.message_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */file_parts fp
      WHERE fp.channel_id = r.channel_id
        AND fp.message_id = r.message_id
  )
ORDER BY r.upload_id, r.channel_id, r.part_no;

-- name: DeleteUploadPartReceiptsForCleanup :execrows
DELETE FROM /* TEMPLATE: schema */upload_part_receipts AS receipt
USING /* TEMPLATE: schema */upload_sessions AS session,
      jsonb_to_recordset(sqlc.arg(receipts)::jsonb) AS cleanup_receipt(
          upload_id uuid,
          part_no integer,
          lease_token uuid,
          channel_id bigint,
          message_id bigint
      )
WHERE receipt.upload_id = cleanup_receipt.upload_id
  AND receipt.part_no = cleanup_receipt.part_no
  AND receipt.lease_token = cleanup_receipt.lease_token
  AND receipt.channel_id = cleanup_receipt.channel_id
  AND receipt.message_id = cleanup_receipt.message_id
  AND session.id = receipt.upload_id
  AND session.state IN ('aborted', 'expired')
  AND receipt.state IN ('published', 'deleting')
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */upload_parts up
      WHERE up.channel_id = receipt.channel_id
        AND up.message_id = receipt.message_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */file_parts fp
      WHERE fp.channel_id = receipt.channel_id
        AND fp.message_id = receipt.message_id
  );

-- name: ListCondemnedReceiptsForCleanup :many
-- Condemned messages can belong to still-open sessions (validation failures or
-- lost leases), so they are swept independently of session state.
SELECT us.user_id AS user_id, r.*
FROM /* TEMPLATE: schema */upload_part_receipts r
JOIN /* TEMPLATE: schema */upload_sessions us ON us.id = r.upload_id
WHERE r.state = 'deleting'
  AND r.message_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */upload_parts up
      WHERE up.channel_id = r.channel_id
        AND up.message_id = r.message_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */file_parts fp
      WHERE fp.channel_id = r.channel_id
        AND fp.message_id = r.message_id
  )
ORDER BY r.updated_at, r.upload_id, r.part_no
LIMIT 1000;

-- name: DeleteCondemnedReceiptsForCleanup :execrows
DELETE FROM /* TEMPLATE: schema */upload_part_receipts AS receipt
USING jsonb_to_recordset(sqlc.arg(receipts)::jsonb) AS cleanup_receipt(
    upload_id uuid,
    part_no integer,
    lease_token uuid,
    channel_id bigint,
    message_id bigint
)
WHERE receipt.upload_id = cleanup_receipt.upload_id
  AND receipt.part_no = cleanup_receipt.part_no
  AND receipt.lease_token = cleanup_receipt.lease_token
  AND receipt.channel_id = cleanup_receipt.channel_id
  AND receipt.message_id = cleanup_receipt.message_id
  AND receipt.state = 'deleting'
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */upload_parts up
      WHERE up.channel_id = receipt.channel_id
        AND up.message_id = receipt.message_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */file_parts fp
      WHERE fp.channel_id = receipt.channel_id
        AND fp.message_id = receipt.message_id
  );

-- name: ListSupersededPublishedReceipts :many
-- Published receipts whose part committed a different message (duplicate
-- publishes after lease takeover); their own message is unreferenced.
SELECT us.user_id AS user_id, r.*
FROM /* TEMPLATE: schema */upload_part_receipts r
JOIN /* TEMPLATE: schema */upload_sessions us ON us.id = r.upload_id
JOIN /* TEMPLATE: schema */upload_parts p ON p.upload_id = r.upload_id AND p.part_no = r.part_no
WHERE r.state = 'published'
  AND r.message_id IS NOT NULL
  AND p.state = 'stored'
  AND p.message_id IS DISTINCT FROM r.message_id
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */upload_parts up
      WHERE up.channel_id = r.channel_id
        AND up.message_id = r.message_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */file_parts fp
      WHERE fp.channel_id = r.channel_id
        AND fp.message_id = r.message_id
  )
ORDER BY r.updated_at, r.upload_id, r.part_no
LIMIT 1000;

-- name: DeleteSupersededReceiptsForCleanup :execrows
DELETE FROM /* TEMPLATE: schema */upload_part_receipts AS receipt
USING /* TEMPLATE: schema */upload_parts AS part,
      jsonb_to_recordset(sqlc.arg(receipts)::jsonb) AS cleanup_receipt(
          upload_id uuid,
          part_no integer,
          lease_token uuid,
          channel_id bigint,
          message_id bigint
      )
WHERE receipt.upload_id = cleanup_receipt.upload_id
  AND receipt.part_no = cleanup_receipt.part_no
  AND receipt.lease_token = cleanup_receipt.lease_token
  AND receipt.channel_id = cleanup_receipt.channel_id
  AND receipt.message_id = cleanup_receipt.message_id
  AND part.upload_id = receipt.upload_id
  AND part.part_no = receipt.part_no
  AND part.state = 'stored'
  AND part.message_id IS DISTINCT FROM receipt.message_id
  AND receipt.state = 'published'
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */upload_parts up
      WHERE up.channel_id = receipt.channel_id
        AND up.message_id = receipt.message_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM /* TEMPLATE: schema */file_parts fp
      WHERE fp.channel_id = receipt.channel_id
        AND fp.message_id = receipt.message_id
  );

-- Committed-class row garbage collection: adopted receipts never require a
-- remote delete. They are removed once the file is committed (file_parts is
-- the durable reference) or their stored part row has already been cleaned.
-- name: DeleteAdoptedReceipts :execrows
WITH gone AS (
    SELECT r.upload_id AS upload_id, r.part_no AS part_no, r.lease_token AS lease_token
    FROM /* TEMPLATE: schema */upload_part_receipts r
    JOIN /* TEMPLATE: schema */upload_sessions us ON us.id = r.upload_id
    WHERE r.state = 'adopted'
      AND (
          us.state = 'completed'
          OR NOT EXISTS (
              SELECT 1
              FROM /* TEMPLATE: schema */upload_parts up
              WHERE up.upload_id = r.upload_id
                AND up.part_no = r.part_no
                AND up.channel_id = r.channel_id
                AND up.message_id = r.message_id
          )
      )
    LIMIT 1000
)
DELETE FROM /* TEMPLATE: schema */upload_part_receipts
WHERE (upload_id, part_no, lease_token) IN (SELECT upload_id, part_no, lease_token FROM gone);
