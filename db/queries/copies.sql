-- name: InsertCopyOperation :one
INSERT INTO /* TEMPLATE: schema */copy_operations (
    id,
    user_id,
    source_root_id,
    target_parent_id,
    requested_name,
    conflict_policy,
    tree_snapshot,
    expires_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(user_id),
    sqlc.arg(source_root_id),
    sqlc.narg(target_parent_id),
    sqlc.narg(requested_name),
    sqlc.arg(conflict_policy),
    sqlc.arg(tree_snapshot)::jsonb,
    sqlc.arg(expires_at)
)
RETURNING *;

-- name: InsertCopyOperationParts :execrows
INSERT INTO /* TEMPLATE: schema */copy_operation_parts (
    operation_id,
    source_file_id,
    part_no,
    source_channel_id,
    source_message_id,
    dest_channel_id,
    plain_size,
    stored_size,
    checksum,
    salt,
    block_hashes
)
SELECT sqlc.arg(operation_id),
       input.source_file_id,
       input.part_no,
       input.source_channel_id,
       input.source_message_id,
       input.dest_channel_id,
       input.plain_size,
       input.stored_size,
       input.checksum,
       input.salt,
       decode(input.block_hashes, 'base64')
FROM jsonb_to_recordset(sqlc.arg(parts)::jsonb) AS input(
    source_file_id uuid,
    part_no integer,
    source_channel_id bigint,
    source_message_id bigint,
    dest_channel_id bigint,
    plain_size bigint,
    stored_size bigint,
    checksum text,
    salt text,
    block_hashes text
);

-- name: GetCopyOperation :one
SELECT *
FROM /* TEMPLATE: schema */copy_operations
WHERE id = sqlc.arg(id);

-- name: ListCopyOperationParts :many
SELECT *
FROM /* TEMPLATE: schema */copy_operation_parts
WHERE operation_id = sqlc.arg(operation_id)
ORDER BY source_file_id, part_no;

-- name: ListStaleCopyOperations :many
SELECT *
FROM /* TEMPLATE: schema */copy_operations
WHERE state = 'parts_pending'
  AND updated_at < sqlc.arg(stale_before)
ORDER BY updated_at, id
LIMIT sqlc.arg(row_limit);

-- A confirmed receipt also heartbeats the operation, which lets recovery
-- distinguish a long but alive copy from a crashed one.
-- name: ConfirmCopyPart :exec
WITH confirmed AS (
    UPDATE /* TEMPLATE: schema */copy_operation_parts
    SET dest_message_id = sqlc.arg(dest_message_id),
        copied_at = now()
    WHERE operation_id = sqlc.arg(operation_id)
      AND source_file_id = sqlc.arg(source_file_id)
      AND part_no = sqlc.arg(part_no)
      AND dest_message_id IS NULL
)
UPDATE /* TEMPLATE: schema */copy_operations
SET updated_at = now()
WHERE id = sqlc.arg(operation_id);

-- name: RecordCopyOperationError :exec
UPDATE /* TEMPLATE: schema */copy_operations
SET last_error = sqlc.narg(last_error),
    updated_at = now()
WHERE id = sqlc.arg(id);

-- name: CompleteCopyOperation :execrows
UPDATE /* TEMPLATE: schema */copy_operations
SET state = 'published',
    result_root_id = sqlc.arg(result_root_id),
    published_at = now(),
    updated_at = now(),
    last_error = NULL
WHERE id = sqlc.arg(id)
  AND state = 'parts_pending';

-- Only an unfinished operation can become aborted; a published operation is
-- immutable so late compensation can never undo a committed catalog tree.
-- name: MarkCopyOperationAborted :execrows
UPDATE /* TEMPLATE: schema */copy_operations
SET state = 'aborted',
    last_error = sqlc.narg(last_error),
    updated_at = now()
WHERE id = sqlc.arg(id)
  AND state = 'parts_pending';

-- name: ListCopyPartReceipts :many
SELECT dest_channel_id AS channel_id,
       dest_message_id AS message_id
FROM /* TEMPLATE: schema */copy_operation_parts
WHERE operation_id = sqlc.arg(operation_id)
  AND dest_message_id IS NOT NULL
ORDER BY dest_channel_id, dest_message_id;

-- Messages held by other live operations are off-limits for compensation:
-- Telegram may have reused the same numeric message id after deletion.
-- name: ListOtherCopyPartMessageClaims :many
SELECT p.dest_message_id AS message_id
FROM /* TEMPLATE: schema */copy_operation_parts AS p
JOIN /* TEMPLATE: schema */copy_operations AS o ON o.id = p.operation_id
WHERE p.operation_id <> sqlc.arg(operation_id)
  AND o.state IN ('parts_pending', 'published')
  AND p.dest_channel_id = sqlc.arg(dest_channel_id)
  AND p.dest_message_id = ANY(sqlc.arg(message_ids)::bigint[]);

-- name: DeleteExpiredCopyOperations :execrows
DELETE FROM /* TEMPLATE: schema */copy_operations
WHERE state IN ('published', 'aborted')
  AND expires_at <= now();
