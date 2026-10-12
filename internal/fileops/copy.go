package fileops

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
)

const (
	// copyOperationTTL bounds how long terminal operation rows are retained for
	// observability before the recovery sweep deletes them.
	copyOperationTTL = 30 * 24 * time.Hour
	// copyRecoveryStaleAge is the minimum heartbeat age before an unfinished
	// operation is considered abandoned to a request cancellation or crash.
	copyRecoveryStaleAge = 30 * time.Minute
	copyRecoveryBatch    = 16
	copyCleanupTimeout   = 2 * time.Minute
	copyErrorLimit       = 1000
)

// ErrCopyOperationAborted is returned when a durable copy has reached the
// terminal aborted state and can no longer be published.
var ErrCopyOperationAborted = errors.New("copy operation aborted")

// errCopyCommitUncertain marks a failed publish transaction whose commit
// result is unknown. The caller must reconcile against the stored operation
// state before touching any remote message.
var errCopyCommitUncertain = errors.New("copy commit result uncertain")

type copiedFileRecord struct {
	ID                   uuid.UUID  `json:"id"`
	UserID               int64      `json:"user_id"`
	ParentID             *uuid.UUID `json:"parent_id"`
	Name                 string     `json:"name"`
	Kind                 string     `json:"kind"`
	MIMEType             *string    `json:"mime_type"`
	Size                 *int64     `json:"size"`
	HashAlgorithm        *string    `json:"hash_algorithm"`
	HashValue            *string    `json:"hash_value"`
	Encryption           bool       `json:"encryption"`
	EncryptionKeyVersion *int32     `json:"encryption_key_version"`
	ModTime              time.Time  `json:"mod_time"`
}

type copiedFilePartRecord struct {
	FileID      uuid.UUID `json:"file_id"`
	PartNo      int32     `json:"part_no"`
	ChannelID   int64     `json:"channel_id"`
	MessageID   int64     `json:"message_id"`
	PlainSize   *int64    `json:"plain_size"`
	StoredSize  *int64    `json:"stored_size"`
	Checksum    *string   `json:"checksum"`
	Salt        *string   `json:"salt"`
	BlockHashes []byte    `json:"block_hashes"`
}

// copySnapshotNode is one pinned source-tree node carrying both the source id
// and the pre-assigned destination id. Recovery publishes exclusively from
// this snapshot and never reloads the (possibly purged) source tree.
type copySnapshotNode struct {
	copiedFileRecord
	SourceID uuid.UUID `json:"source_id"`
}

// copyPartPlanRecord is the per-part plan. Source and destination channels are
// pinned up front so retries never reselect; dest_message_id is filled in by
// the confirmed Telegram receipt.
type copyPartPlanRecord struct {
	SourceFileID    uuid.UUID `json:"source_file_id"`
	PartNo          int32     `json:"part_no"`
	SourceChannelID int64     `json:"source_channel_id"`
	SourceMessageID int64     `json:"source_message_id"`
	DestChannelID   int64     `json:"dest_channel_id"`
	PlainSize       *int64    `json:"plain_size"`
	StoredSize      *int64    `json:"stored_size"`
	Checksum        *string   `json:"checksum"`
	Salt            *string   `json:"salt"`
	BlockHashes     []byte    `json:"block_hashes"`
}

func (s *Service) Copy(ctx context.Context, in CopyInput) (*sqlcgen.File, error) {
	if in.UserID <= 0 || in.FileID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	if in.ConflictPolicy == "" {
		in.ConflictPolicy = sqlcgen.NameConflictPolicyFail
	}
	switch in.ConflictPolicy {
	case sqlcgen.NameConflictPolicyFail, sqlcgen.NameConflictPolicyRename, sqlcgen.NameConflictPolicyReplace:
	default:
		return nil, ErrInvalidInput
	}
	if in.ParentID != nil {
		parent, err := s.catalog.Get(ctx, in.UserID, *in.ParentID)
		if err != nil {
			return nil, err
		}
		if parent.Kind != sqlcgen.FileKindFolder || parent.Status != sqlcgen.FileStatusActive {
			return nil, catalog.ErrInvalidParent
		}
	}
	nodes, err := s.loadTree(ctx, in.UserID, in.FileID)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 || nodes[0].File.Status != sqlcgen.FileStatusActive {
		return nil, ErrNotFound
	}

	idMap := make(map[uuid.UUID]uuid.UUID, len(nodes))
	for _, node := range nodes {
		oldID, ok := dbtypes.GoogleUUID(node.File.ID)
		if !ok {
			return nil, ErrNotFound
		}
		idMap[oldID] = uuid.New()
	}
	rootOldID, _ := dbtypes.GoogleUUID(nodes[0].File.ID)
	rootNewID := idMap[rootOldID]

	sourceFileIDs := make([]uuid.UUID, 0)
	for _, node := range nodes {
		if node.File.Kind != sqlcgen.FileKindFile {
			continue
		}
		if node.File.Status != sqlcgen.FileStatusActive {
			return nil, catalog.ErrNotAFile
		}
		oldID, _ := dbtypes.GoogleUUID(node.File.ID)
		sourceFileIDs = append(sourceFileIDs, oldID)
	}
	parts, err := s.queries.ListFilePartsByFileIDs(ctx, pgUUIDs(sourceFileIDs))
	if err != nil {
		return nil, fmt.Errorf("list copied file parts: %w", err)
	}
	// Channel allocation is pinned into the plan so retries never allocate a
	// different (or auto-created) set of destination channels.
	destinationChannels, err := s.channels.ResolveMany(ctx, in.UserID, len(parts))
	if err != nil {
		return nil, err
	}
	op, err := s.createCopyOperation(ctx, in, nodes, parts, destinationChannels, idMap)
	if err != nil {
		return nil, err
	}
	file, _, err := s.driveCopyOperation(ctx, op)
	if err != nil {
		return nil, err
	}
	if id, ok := dbtypes.GoogleUUID(file.ID); !ok || id != rootNewID {
		return nil, ErrNotFound
	}
	return file, nil
}

func (s *Service) createCopyOperation(
	ctx context.Context,
	in CopyInput,
	nodes []treeNode,
	parts []*sqlcgen.FilePart,
	destinationChannels []int64,
	idMap map[uuid.UUID]uuid.UUID,
) (*sqlcgen.CopyOperation, error) {
	snapshot := make([]copySnapshotNode, 0, len(nodes))
	for _, node := range nodes {
		oldID, ok := dbtypes.GoogleUUID(node.File.ID)
		if !ok {
			return nil, ErrNotFound
		}
		var parentID *uuid.UUID
		if oldID == in.FileID {
			// The root is re-parented onto the copy destination.
			parentID = in.ParentID
		} else if oldParent, ok := dbtypes.GoogleUUID(node.File.ParentID); ok {
			mapped := idMap[oldParent]
			parentID = &mapped
		}
		snapshot = append(snapshot, copySnapshotNode{
			SourceID: oldID,
			copiedFileRecord: copiedFileRecord{
				ID: idMap[oldID], UserID: in.UserID, ParentID: parentID, Name: node.File.Name,
				Kind:     string(node.File.Kind),
				MIMEType: optionalText(node.File.MimeType), Size: optionalInt64(node.File.Size),
				HashAlgorithm: optionalText(node.File.HashAlgorithm), HashValue: optionalText(node.File.HashValue),
				Encryption: node.File.Encryption, EncryptionKeyVersion: optionalInt32(node.File.EncryptionKeyVersion),
				ModTime: node.File.ModTime.Time,
			},
		})
	}
	planned := make([]copyPartPlanRecord, 0, len(parts))
	for index, part := range parts {
		sourceID, ok := dbtypes.GoogleUUID(part.FileID)
		if !ok {
			return nil, ErrNotFound
		}
		planned = append(planned, copyPartPlanRecord{
			SourceFileID: sourceID, PartNo: part.PartNo,
			SourceChannelID: part.ChannelID, SourceMessageID: part.MessageID,
			DestChannelID: destinationChannels[index],
			PlainSize:     optionalInt64(part.PlainSize), StoredSize: optionalInt64(part.StoredSize),
			Checksum: optionalText(part.Checksum), Salt: optionalText(part.Salt),
			BlockHashes: part.BlockHashes,
		})
	}
	encodedSnapshot, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("encode copy tree snapshot: %w", err)
	}
	encodedParts, err := json.Marshal(planned)
	if err != nil {
		return nil, fmt.Errorf("encode copy part plan: %w", err)
	}
	opID := uuid.New()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	q := s.queries.WithTx(tx)
	op, err := q.InsertCopyOperation(ctx, sqlcgen.InsertCopyOperationParams{
		ID: dbtypes.UUID(opID), UserID: in.UserID, SourceRootID: dbtypes.UUID(in.FileID),
		TargetParentID: dbtypes.OptionalUUID(in.ParentID), RequestedName: dbtypes.OptionalText(in.Name),
		ConflictPolicy: in.ConflictPolicy, TreeSnapshot: encodedSnapshot,
		ExpiresAt: dbtypes.Time(time.Now().Add(copyOperationTTL)),
	})
	if err != nil {
		return nil, fmt.Errorf("create copy operation: %w", err)
	}
	if len(planned) > 0 {
		inserted, err := q.InsertCopyOperationParts(ctx, sqlcgen.InsertCopyOperationPartsParams{
			OperationID: op.ID, Parts: encodedParts,
		})
		if err != nil {
			return nil, fmt.Errorf("create copy part plan: %w", err)
		}
		if inserted != int64(len(planned)) {
			return nil, ErrNotFound
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit copy operation: %w", err)
	}
	return op, nil
}

// driveCopyOperation resumes a durable copy from its persisted state: only
// unconfirmed parts are copied remotely and the catalog tree is published in a
// single conflict-serialized transaction.
func (s *Service) driveCopyOperation(ctx context.Context, op *sqlcgen.CopyOperation) (*sqlcgen.File, []uuid.UUID, error) {
	if op.State == sqlcgen.CopyOperationStatePublished {
		file, err := s.fetchCopyResult(ctx, op)
		return file, nil, err
	}
	if op.State == sqlcgen.CopyOperationStateAborted {
		return nil, nil, ErrCopyOperationAborted
	}
	planned, err := s.queries.ListCopyOperationParts(ctx, op.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("list copy part plan: %w", err)
	}
	if err := s.copyRemoteParts(ctx, op, planned); err != nil {
		// Best effort: cleanup failures keep the operation pending for recovery.
		_ = s.abortCopyOperation(op, err)
		return nil, nil, err
	}
	file, replacedIDs, publishErr := s.publishCopy(ctx, op, planned)
	if publishErr == nil {
		return file, replacedIDs, nil
	}
	if errors.Is(publishErr, errCopyCommitUncertain) {
		switch file, resolved := s.resolvePublishedCopy(op); resolved {
		case publishedConfirmed:
			return file, nil, nil
		case publishedAbsent:
			if abortErr := s.abortCopyOperation(op, publishErr); abortErr != nil {
				return nil, nil, errors.Join(publishErr, abortErr)
			}
			return nil, nil, errors.Unwrap(publishErr)
		default:
			// The database cannot tell whether the commit landed. Leave the
			// operation pending and observable; the recovery sweep converges it.
			return nil, nil, publishErr
		}
	}
	if abortErr := s.abortCopyOperation(op, publishErr); abortErr != nil {
		return nil, nil, errors.Join(publishErr, abortErr)
	}
	return nil, nil, publishErr
}

type copyPublishResolution int

const (
	publishedUnknown copyPublishResolution = iota
	publishedConfirmed
	publishedAbsent
)

func (s *Service) resolvePublishedCopy(op *sqlcgen.CopyOperation) (*sqlcgen.File, copyPublishResolution) {
	ctx, cancel := context.WithTimeout(context.Background(), copyCleanupTimeout)
	defer cancel()
	fresh, err := s.queries.GetCopyOperation(ctx, op.ID)
	if err != nil {
		return nil, publishedUnknown
	}
	if fresh.State != sqlcgen.CopyOperationStatePublished {
		return nil, publishedAbsent
	}
	file, err := s.fetchCopyResult(ctx, fresh)
	if err != nil {
		return nil, publishedUnknown
	}
	if id, ok := dbtypes.GoogleUUID(fresh.ResultRootID); ok {
		s.catalog.InvalidateFiles(ctx, fresh.UserID, id)
	}
	return file, publishedConfirmed
}

func (s *Service) copyRemoteParts(ctx context.Context, op *sqlcgen.CopyOperation, planned []*sqlcgen.CopyOperationPart) error {
	for _, part := range planned {
		if part.DestMessageID.Valid {
			// Confirmed receipt: never copy this part again.
			continue
		}
		stored, err := s.storage.CopyPart(ctx, op.UserID, part.SourceChannelID, part.SourceMessageID, part.DestChannelID)
		if err != nil {
			return fmt.Errorf("copy Telegram part %d: %w", part.PartNo, err)
		}
		if !part.StoredSize.Valid || stored.Size != part.StoredSize.Int64 {
			return fmt.Errorf("copy Telegram part %d: %w", part.PartNo, telegramstore.ErrSizeMismatch)
		}
		if err := s.queries.ConfirmCopyPart(ctx, sqlcgen.ConfirmCopyPartParams{
			OperationID: op.ID, SourceFileID: part.SourceFileID, PartNo: part.PartNo,
			DestMessageID: dbtypes.Int8(stored.MessageID),
		}); err != nil {
			return fmt.Errorf("record Telegram part %d receipt: %w", part.PartNo, err)
		}
		part.DestMessageID = dbtypes.Int8(stored.MessageID)
	}
	return nil
}

func (s *Service) publishCopy(ctx context.Context, op *sqlcgen.CopyOperation, planned []*sqlcgen.CopyOperationPart) (*sqlcgen.File, []uuid.UUID, error) {
	opID, ok := dbtypes.GoogleUUID(op.ID)
	if !ok {
		return nil, nil, ErrNotFound
	}
	var snapshot []copySnapshotNode
	if err := json.Unmarshal(op.TreeSnapshot, &snapshot); err != nil {
		return nil, nil, fmt.Errorf("decode copy tree snapshot: %w", err)
	}
	rootSourceID, _ := dbtypes.GoogleUUID(op.SourceRootID)
	rootNewID := uuid.Nil
	for _, node := range snapshot {
		if node.SourceID == rootSourceID {
			rootNewID = node.ID
		}
	}
	if rootNewID == uuid.Nil {
		return nil, nil, ErrNotFound
	}
	var targetParentID *uuid.UUID
	if id, ok := dbtypes.GoogleUUID(op.TargetParentID); ok {
		targetParentID = &id
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)
	q := s.queries.WithTx(tx)
	// Own the operation before the destination so concurrent drives and
	// compensations of the same copy never interleave.
	if err := q.AcquireAdvisoryTransactionLock(ctx, copyOperationLockID(opID)); err != nil {
		return nil, nil, fmt.Errorf("lock copy operation: %w", err)
	}
	if err := q.AcquireAdvisoryTransactionLock(ctx, copyDestinationLockID(op.UserID, targetParentID)); err != nil {
		return nil, nil, fmt.Errorf("lock copy destination: %w", err)
	}
	fresh, err := q.GetCopyOperation(ctx, op.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("reload copy operation: %w", err)
	}
	if fresh.State == sqlcgen.CopyOperationStatePublished {
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, markCommitUncertain(err)
		}
		file, err := s.fetchCopyResult(ctx, fresh)
		return file, nil, err
	}
	if fresh.State == sqlcgen.CopyOperationStateAborted {
		return nil, nil, ErrCopyOperationAborted
	}

	rootName := rootSnapshotName(snapshot, rootSourceID)
	if op.RequestedName.Valid {
		rootName = op.RequestedName.String
	}
	conflict, conflictErr := q.LockUploadDestinationConflict(ctx, sqlcgen.LockUploadDestinationConflictParams{
		UserID: op.UserID, ParentID: dbtypes.OptionalUUID(targetParentID), Name: rootName,
	})
	if conflictErr != nil && !errors.Is(conflictErr, pgx.ErrNoRows) {
		return nil, nil, fmt.Errorf("check copy destination conflict: %w", conflictErr)
	}
	var replacedIDs []uuid.UUID
	if conflictErr == nil {
		conflictID, ok := dbtypes.GoogleUUID(conflict.ID)
		if !ok {
			return nil, nil, catalog.ErrConflict
		}
		switch op.ConflictPolicy {
		case sqlcgen.NameConflictPolicyFail:
			return nil, nil, catalog.ErrConflict
		case sqlcgen.NameConflictPolicyRename:
			rootName, err = nextAvailableCopyName(ctx, q, op.UserID, targetParentID, rootName)
			if err != nil {
				return nil, nil, err
			}
		case sqlcgen.NameConflictPolicyReplace:
			replacedIDs, err = replaceCopyDestination(ctx, q, op.UserID, conflictID)
			if err != nil {
				return nil, nil, err
			}
		default:
			return nil, nil, ErrInvalidInput
		}
	}

	fileRecords := make([]copiedFileRecord, 0, len(snapshot))
	for _, node := range snapshot {
		record := node.copiedFileRecord
		if node.SourceID == rootSourceID {
			record.Name = rootName
		}
		fileRecords = append(fileRecords, record)
	}
	newIDBySource := make(map[uuid.UUID]uuid.UUID, len(snapshot))
	for _, node := range snapshot {
		newIDBySource[node.SourceID] = node.ID
	}
	partRecords := make([]copiedFilePartRecord, 0, len(planned))
	for _, part := range planned {
		if !part.DestMessageID.Valid {
			return nil, nil, fmt.Errorf("copy part %d has no confirmed Telegram receipt", part.PartNo)
		}
		sourceID, ok := dbtypes.GoogleUUID(part.SourceFileID)
		if !ok {
			return nil, nil, ErrNotFound
		}
		newFileID, ok := newIDBySource[sourceID]
		if !ok {
			return nil, nil, ErrNotFound
		}
		partRecords = append(partRecords, copiedFilePartRecord{
			FileID: newFileID, PartNo: part.PartNo, ChannelID: part.DestChannelID,
			MessageID: part.DestMessageID.Int64, PlainSize: optionalInt64(part.PlainSize),
			StoredSize: optionalInt64(part.StoredSize), Checksum: optionalText(part.Checksum),
			Salt: optionalText(part.Salt), BlockHashes: part.BlockHashes,
		})
	}

	encodedFiles, err := json.Marshal(fileRecords)
	if err != nil {
		return nil, nil, fmt.Errorf("encode copied files: %w", err)
	}
	insertedFiles, err := q.InsertCopiedFiles(ctx, encodedFiles)
	if err != nil {
		return nil, nil, fmt.Errorf("insert copied catalog rows: %w", err)
	}
	if len(insertedFiles) != len(fileRecords) {
		return nil, nil, ErrNotFound
	}
	if len(partRecords) > 0 {
		encodedParts, err := json.Marshal(partRecords)
		if err != nil {
			return nil, nil, fmt.Errorf("encode copied file parts: %w", err)
		}
		insertedParts, err := q.InsertCopiedFileParts(ctx, encodedParts)
		if err != nil {
			return nil, nil, fmt.Errorf("insert copied file parts: %w", err)
		}
		if insertedParts != int64(len(partRecords)) {
			return nil, nil, ErrNotFound
		}
	}
	completed, err := q.CompleteCopyOperation(ctx, sqlcgen.CompleteCopyOperationParams{
		ID: op.ID, ResultRootID: dbtypes.UUID(rootNewID),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("complete copy operation: %w", err)
	}
	if completed != 1 {
		return nil, nil, fmt.Errorf("complete copy operation: %w", ErrCopyOperationAborted)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, markCommitUncertain(err)
	}
	s.catalog.InvalidateFiles(ctx, op.UserID, replacedIDs...)
	file, err := s.fetchCopyResult(ctx, &sqlcgen.CopyOperation{
		ID: op.ID, UserID: op.UserID, State: sqlcgen.CopyOperationStatePublished,
		ResultRootID: dbtypes.UUID(rootNewID),
	})
	if err != nil {
		return nil, nil, err
	}
	return file, replacedIDs, nil
}

func replaceCopyDestination(ctx context.Context, q *sqlcgen.Queries, userID int64, conflictID uuid.UUID) ([]uuid.UUID, error) {
	subtree, err := q.ListFileSubtreeIDs(ctx, sqlcgen.ListFileSubtreeIDsParams{
		FileID: dbtypes.UUID(conflictID), UserID: userID,
	})
	if err != nil {
		return nil, fmt.Errorf("list replaced copy destination subtree: %w", err)
	}
	replacedIDs := make([]uuid.UUID, 0, len(subtree))
	for _, subtreeID := range subtree {
		if value, ok := dbtypes.GoogleUUID(subtreeID); ok {
			replacedIDs = append(replacedIDs, value)
		}
	}
	if err := q.MarkFileSubtreeDeletionPending(ctx, sqlcgen.MarkFileSubtreeDeletionPendingParams{
		UserID: userID, FileID: dbtypes.UUID(conflictID),
	}); err != nil {
		return nil, fmt.Errorf("mark replaced copy destination for deletion: %w", err)
	}
	if err := q.RevokeSharesForFileSubtree(ctx, sqlcgen.RevokeSharesForFileSubtreeParams{
		UserID: userID, FileID: dbtypes.UUID(conflictID),
	}); err != nil {
		return nil, fmt.Errorf("revoke replaced copy destination shares: %w", err)
	}
	return replacedIDs, nil
}

func rootSnapshotName(snapshot []copySnapshotNode, rootSourceID uuid.UUID) string {
	for _, node := range snapshot {
		if node.SourceID == rootSourceID {
			return node.Name
		}
	}
	return ""
}

func (s *Service) fetchCopyResult(ctx context.Context, op *sqlcgen.CopyOperation) (*sqlcgen.File, error) {
	rootID, ok := dbtypes.GoogleUUID(op.ResultRootID)
	if !ok {
		return nil, ErrNotFound
	}
	file, err := s.queries.GetFileForUser(ctx, sqlcgen.GetFileForUserParams{
		FileID: dbtypes.UUID(rootID), UserID: op.UserID,
	})
	if err != nil {
		return nil, fmt.Errorf("load published copy result: %w", err)
	}
	return file, nil
}

func markCommitUncertain(err error) error {
	return fmt.Errorf("%w: %v", errCopyCommitUncertain, err)
}

// abortCopyOperation safely recycles only this operation's unpublished
// messages. Every candidate is re-checked against published catalog/upload
// references and against receipts of other live operations, and the operation
// is marked aborted only after remote cleanup succeeds. It always runs on a
// detached context so request cancellation cannot interrupt compensation.
func (s *Service) abortCopyOperation(op *sqlcgen.CopyOperation, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), copyCleanupTimeout)
	defer cancel()

	opID, ok := dbtypes.GoogleUUID(op.ID)
	if !ok {
		return ErrNotFound
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire copy abort connection: %w", err)
	}
	defer conn.Release()
	q := sqlcgen.New(conn)
	lockID := copyOperationLockID(opID)
	if err := q.AcquireAdvisoryLock(ctx, lockID); err != nil {
		return fmt.Errorf("acquire copy abort lock: %w", err)
	}
	defer func() {
		unlockCtx, cancelUnlock := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelUnlock()
		_, _ = q.ReleaseAdvisoryLock(unlockCtx, lockID)
	}()

	fresh, err := q.GetCopyOperation(ctx, op.ID)
	if err != nil {
		return fmt.Errorf("reload aborting copy operation: %w", err)
	}
	if fresh.State != sqlcgen.CopyOperationStatePartsPending {
		// Published rows are immutable; a duplicate abort is harmless.
		return nil
	}

	receipts, err := q.ListCopyPartReceipts(ctx, op.ID)
	if err != nil {
		return fmt.Errorf("list copy receipts for cleanup: %w", err)
	}
	grouped := make(map[int64][]int64)
	for _, receipt := range receipts {
		if receipt.ChannelID != 0 && receipt.MessageID.Valid && receipt.MessageID.Int64 > 0 {
			grouped[receipt.ChannelID] = append(grouped[receipt.ChannelID], receipt.MessageID.Int64)
		}
	}
	for channelID, messageIDs := range grouped {
		protected, err := s.protectedCopyMessages(ctx, q, op.ID, channelID, messageIDs)
		if err != nil {
			return err
		}
		deletable := make([]int64, 0, len(messageIDs))
		for _, messageID := range messageIDs {
			if _, ok := protected[messageID]; ok {
				continue
			}
			deletable = append(deletable, messageID)
		}
		if len(deletable) == 0 {
			continue
		}
		if err := s.storage.DeleteMessages(ctx, fresh.UserID, channelID, deletable); err != nil {
			_ = q.RecordCopyOperationError(ctx, sqlcgen.RecordCopyOperationErrorParams{
				ID: op.ID, LastError: dbtypes.Text(copyErrorMessage(cause)),
			})
			return fmt.Errorf("recycle unpublished copy messages in channel %d: %w", channelID, err)
		}
	}
	marked, err := q.MarkCopyOperationAborted(ctx, sqlcgen.MarkCopyOperationAbortedParams{
		ID: op.ID, LastError: dbtypes.Text(copyErrorMessage(cause)),
	})
	if err != nil {
		return fmt.Errorf("mark copy operation aborted: %w", err)
	}
	if marked == 0 {
		// A concurrent drive may have published. Late compensation must stop.
		final, getErr := q.GetCopyOperation(ctx, op.ID)
		if getErr == nil && final.State == sqlcgen.CopyOperationStatePublished {
			return nil
		}
		return fmt.Errorf("copy operation state changed before abort")
	}
	return nil
}

// protectedCopyMessages reports which of the operation's candidate messages
// must not be deleted because the catalog (file_parts or upload_parts) or
// another live copy operation already references them.
func (s *Service) protectedCopyMessages(
	ctx context.Context,
	q *sqlcgen.Queries,
	operationID pgtype.UUID,
	channelID int64,
	messageIDs []int64,
) (map[int64]struct{}, error) {
	protected := make(map[int64]struct{}, len(messageIDs))
	refs, err := q.ListReferencedMessageIDs(ctx, sqlcgen.ListReferencedMessageIDsParams{
		TargetChannelID: channelID, MessageIds: messageIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("check catalog references for copied messages in channel %d: %w", channelID, err)
	}
	for _, ref := range refs {
		protected[ref] = struct{}{}
	}
	claims, err := q.ListOtherCopyPartMessageClaims(ctx, sqlcgen.ListOtherCopyPartMessageClaimsParams{
		OperationID: operationID, DestChannelID: channelID, MessageIds: messageIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("check other copy operations for copied messages in channel %d: %w", channelID, err)
	}
	for _, claim := range claims {
		if claim.Valid {
			protected[claim.Int64] = struct{}{}
		}
	}
	return protected, nil
}

func copyErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) > copyErrorLimit {
		return message[:copyErrorLimit]
	}
	return message
}

// RecoverPendingCopies resumes stale, unfinished copies using their pinned
// snapshots and receipts. It is safe to call concurrently with request
// handling; per-operation advisory locks decide ownership.
func (s *Service) RecoverPendingCopies(ctx context.Context, staleBefore time.Time, limit int32) (int, error) {
	if staleBefore.IsZero() {
		staleBefore = time.Now().Add(-copyRecoveryStaleAge)
	}
	if limit <= 0 {
		limit = copyRecoveryBatch
	}
	ops, err := s.queries.ListStaleCopyOperations(ctx, sqlcgen.ListStaleCopyOperationsParams{
		StaleBefore: dbtypes.Time(staleBefore), RowLimit: limit,
	})
	if err != nil {
		return 0, fmt.Errorf("list stale copy operations: %w", err)
	}
	recovered := 0
	for _, op := range ops {
		file, replacedIDs, driveErr := s.driveCopyOperation(ctx, op)
		if driveErr != nil {
			return recovered, fmt.Errorf("recover copy operation %s: %w", op.ID.String(), driveErr)
		}
		if file != nil && len(replacedIDs) > 0 {
			s.catalog.InvalidateFiles(ctx, op.UserID, replacedIDs...)
		}
		recovered++
	}
	return recovered, nil
}

// DeleteExpiredCopyOperations removes terminal operation rows past their
// retention window.
func (s *Service) DeleteExpiredCopyOperations(ctx context.Context) (int64, error) {
	deleted, err := s.queries.DeleteExpiredCopyOperations(ctx)
	if err != nil {
		return 0, fmt.Errorf("delete expired copy operations: %w", err)
	}
	return deleted, nil
}

// copyOperationLockID derives the stable advisory lock for a copy operation.
func copyOperationLockID(operationID uuid.UUID) int64 {
	input := append([]byte("teldrive/copy-operation/"), operationID[:]...)
	digest := sha256.Sum256(input)
	return int64(binary.BigEndian.Uint64(digest[:8]))
}
