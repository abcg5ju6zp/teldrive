package jobs

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
)

const (
	UploadCleanupSweepKind = "teldrive_cleanup_uploads"
	CleanupQueue           = "maintenance"
)

var ErrUploadCleanupNotConfigured = errors.New("upload cleanup worker is not configured")

// Deprecated compatibility names; new code should use the explicit upload-cleanup names.
const CleanupSweepKind = UploadCleanupSweepKind

var ErrCleanupNotConfigured = ErrUploadCleanupNotConfigured

type UploadCleanupSweepArgs struct{}

type CleanupSweepArgs = UploadCleanupSweepArgs

func (UploadCleanupSweepArgs) Kind() string { return UploadCleanupSweepKind }

func (UploadCleanupSweepArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       CleanupQueue,
		MaxAttempts: 3,
		Priority:    2,
	}
}

type UploadCleanupWorker struct {
	river.WorkerDefaults[UploadCleanupSweepArgs]
	pool    *pgxpool.Pool
	queries *sqlcgen.Queries
	storage telegramstore.Storage
}

type cleanupChannel struct {
	userID    int64
	channelID int64
}

type cleanupPartRecord struct {
	UploadID  uuid.UUID `json:"upload_id"`
	PartNo    int32     `json:"part_no"`
	MessageID int64     `json:"message_id"`
}

type cleanupReceiptRecord struct {
	UploadID   uuid.UUID `json:"upload_id"`
	PartNo     int32     `json:"part_no"`
	LeaseToken uuid.UUID `json:"lease_token"`
	ChannelID  int64     `json:"channel_id"`
	MessageID  int64     `json:"message_id"`
}

// cleanupReceipt is a remote message that only exists because of a receipt.
type cleanupReceipt struct {
	UploadID   uuid.UUID
	PartNo     int32
	LeaseToken uuid.UUID
	ChannelID  int64
	MessageID  int64
}

type channelBatch struct {
	key        cleanupChannel
	messageIDs map[int64]struct{}
	parts      []cleanupPartRecord
	receipts   []cleanupReceiptRecord
}

func NewUploadCleanupWorker(pool *pgxpool.Pool, storage telegramstore.Storage) *UploadCleanupWorker {
	return &UploadCleanupWorker{pool: pool, queries: sqlcgen.New(pool), storage: storage}
}

func (w *UploadCleanupWorker) Timeout(*river.Job[UploadCleanupSweepArgs]) time.Duration {
	return 2 * time.Hour
}

func (w *UploadCleanupWorker) Work(ctx context.Context, job *river.Job[UploadCleanupSweepArgs]) error {
	if w.pool == nil || w.storage == nil {
		return ErrUploadCleanupNotConfigured
	}
	for {
		expired, err := w.queries.ExpireUploadSessions(ctx)
		if err != nil {
			return fmt.Errorf("expire upload sessions: %w", err)
		}
		deadPending, err := w.queries.DeleteDeadPendingReceipts(ctx)
		if err != nil {
			return fmt.Errorf("delete dead pending upload receipts: %w", err)
		}
		sessions, err := w.queries.ListUploadSessionsPendingCleanup(ctx)
		if err != nil {
			return fmt.Errorf("list upload cleanup sessions: %w", err)
		}
		if err := w.cleanupTerminalSessions(ctx, sessions); err != nil {
			return err
		}
		condemned, err := w.queries.ListCondemnedReceiptsForCleanup(ctx)
		if err != nil {
			return fmt.Errorf("list condemned upload receipts: %w", err)
		}
		if err := w.cleanupCondemnedReceipts(ctx, condemned); err != nil {
			return err
		}
		superseded, err := w.queries.ListSupersededPublishedReceipts(ctx)
		if err != nil {
			return fmt.Errorf("list superseded upload receipts: %w", err)
		}
		if err := w.cleanupSupersededReceipts(ctx, superseded); err != nil {
			return err
		}
		adopted, err := w.queries.DeleteAdoptedReceipts(ctx)
		if err != nil {
			return fmt.Errorf("garbage collect adopted upload receipts: %w", err)
		}
		if len(expired) == 0 && len(sessions) == 0 && deadPending == 0 &&
			len(condemned) == 0 && len(superseded) == 0 && adopted == 0 {
			return nil
		}
	}
}

// cleanupTerminalSessions removes remote messages of aborted or expired
// sessions, merging stored parts and safe-to-delete receipts into one
// DeleteMessages call per channel.
func (w *UploadCleanupWorker) cleanupTerminalSessions(ctx context.Context, sessions []*sqlcgen.UploadSession) error {
	if len(sessions) == 0 {
		return nil
	}
	userByUpload := make(map[uuid.UUID]int64, len(sessions))
	uploadIDs := make([]pgtype.UUID, 0, len(sessions))
	for _, session := range sessions {
		uploadID, ok := dbtypes.GoogleUUID(session.ID)
		if !ok {
			return errors.New("cleanup session has invalid upload id")
		}
		userByUpload[uploadID] = session.UserID
		uploadIDs = append(uploadIDs, session.ID)
	}
	batches := make(map[cleanupChannel]*channelBatch)
	parts, err := w.queries.ListUploadPartsForCleanupMany(ctx, uploadIDs)
	if err != nil {
		return fmt.Errorf("list upload cleanup parts: %w", err)
	}
	for _, part := range parts {
		if !part.MessageID.Valid || part.MessageID.Int64 <= 0 {
			continue
		}
		uploadID, ok := dbtypes.GoogleUUID(part.UploadID)
		if !ok {
			return errors.New("cleanup part has invalid upload id")
		}
		userID, ok := userByUpload[uploadID]
		if !ok {
			return errors.New("cleanup part has no upload session")
		}
		batch := w.batch(batches, cleanupChannel{userID: userID, channelID: part.ChannelID})
		batch.messageIDs[part.MessageID.Int64] = struct{}{}
		batch.parts = append(batch.parts, cleanupPartRecord{UploadID: uploadID, PartNo: part.PartNo, MessageID: part.MessageID.Int64})
	}
	receipts, err := w.queries.ListUploadPartReceiptsForCleanupMany(ctx, uploadIDs)
	if err != nil {
		return fmt.Errorf("list upload cleanup receipts: %w", err)
	}
	for _, receipt := range receipts {
		uploadID, ok := dbtypes.GoogleUUID(receipt.UploadID)
		if !ok {
			return errors.New("cleanup receipt has invalid upload id")
		}
		record, ok := cleanupReceiptFromRow(receipt.PartNo, receipt.LeaseToken, receipt.ChannelID, receipt.MessageID)
		if !ok {
			return errors.New("cleanup receipt has invalid identity")
		}
		batch := w.batch(batches, cleanupChannel{userID: receipt.UserID, channelID: receipt.ChannelID})
		batch.messageIDs[record.MessageID] = struct{}{}
		batch.receipts = append(batch.receipts, cleanupReceiptRecord{
			UploadID:   uploadID,
			PartNo:     record.PartNo,
			LeaseToken: record.LeaseToken,
			ChannelID:  record.ChannelID,
			MessageID:  record.MessageID,
		})
	}
	return w.flushBatches(ctx, batches, func(batch *channelBatch, encodedReceipts []byte) error {
		if len(batch.parts) > 0 {
			encodedParts, err := json.Marshal(batch.parts)
			if err != nil {
				return fmt.Errorf("encode cleaned upload parts: %w", err)
			}
			deleted, err := w.queries.DeleteUploadPartsForCleanup(ctx, encodedParts)
			if err != nil {
				return fmt.Errorf("delete upload parts after Telegram cleanup: %w", err)
			}
			if deleted != int64(len(batch.parts)) {
				return fmt.Errorf("%d upload parts changed during cleanup", int64(len(batch.parts))-deleted)
			}
		}
		if len(batch.receipts) > 0 {
			deleted, err := w.queries.DeleteUploadPartReceiptsForCleanup(ctx, encodedReceipts)
			if err != nil {
				return fmt.Errorf("delete upload receipts after Telegram cleanup: %w", err)
			}
			if deleted != int64(len(batch.receipts)) {
				return fmt.Errorf("%d upload receipts changed during cleanup", int64(len(batch.receipts))-deleted)
			}
		}
		return nil
	})
}

func (w *UploadCleanupWorker) cleanupCondemnedReceipts(ctx context.Context, rows []*sqlcgen.ListCondemnedReceiptsForCleanupRow) error {
	receipts := make([]cleanupReceipt, 0, len(rows))
	for _, row := range rows {
		receipt, ok := cleanupReceiptFromRow(row.PartNo, row.LeaseToken, row.ChannelID, row.MessageID)
		if !ok {
			return errors.New("condemned receipt has invalid identity")
		}
		receipt.UploadID, _ = dbtypes.GoogleUUID(row.UploadID)
		receipts = append(receipts, receipt)
	}
	return w.deleteIndependentReceipts(ctx, receipts, func(encoded []byte) (int64, error) {
		return w.queries.DeleteCondemnedReceiptsForCleanup(ctx, encoded)
	}, "condemned")
}

func (w *UploadCleanupWorker) cleanupSupersededReceipts(ctx context.Context, rows []*sqlcgen.ListSupersededPublishedReceiptsRow) error {
	receipts := make([]cleanupReceipt, 0, len(rows))
	for _, row := range rows {
		receipt, ok := cleanupReceiptFromRow(row.PartNo, row.LeaseToken, row.ChannelID, row.MessageID)
		if !ok {
			return errors.New("superseded receipt has invalid identity")
		}
		receipt.UploadID, _ = dbtypes.GoogleUUID(row.UploadID)
		receipts = append(receipts, receipt)
	}
	return w.deleteIndependentReceipts(ctx, receipts, func(encoded []byte) (int64, error) {
		return w.queries.DeleteSupersededReceiptsForCleanup(ctx, encoded)
	}, "superseded")
}

// deleteIndependentReceipts sweeps receipts regardless of session state
// (condemned validations on open sessions, or duplicates superseded by a
// differently stored part).
func (w *UploadCleanupWorker) deleteIndependentReceipts(ctx context.Context, receipts []cleanupReceipt, delete func([]byte) (int64, error), kind string) error {
	if len(receipts) == 0 {
		return nil
	}
	batches := make(map[cleanupChannel]*channelBatch)
	for _, receipt := range receipts {
		session, err := w.queries.GetUploadSessionAnyOwner(ctx, dbtypes.UUID(receipt.UploadID))
		if err != nil {
			return fmt.Errorf("get cleanup receipt session: %w", err)
		}
		batch := w.batch(batches, cleanupChannel{userID: session.UserID, channelID: receipt.ChannelID})
		batch.messageIDs[receipt.MessageID] = struct{}{}
		batch.receipts = append(batch.receipts, cleanupReceiptRecord(receipt))
	}
	return w.flushBatches(ctx, batches, func(batch *channelBatch, encodedReceipts []byte) error {
		if len(batch.receipts) == 0 {
			return nil
		}
		deleted, err := delete(encodedReceipts)
		if err != nil {
			return fmt.Errorf("delete %s upload receipts after Telegram cleanup: %w", kind, err)
		}
		if deleted != int64(len(batch.receipts)) {
			return fmt.Errorf("%d %s upload receipts changed during cleanup", int64(len(batch.receipts))-deleted, kind)
		}
		return nil
	})
}

func (w *UploadCleanupWorker) batch(batches map[cleanupChannel]*channelBatch, key cleanupChannel) *channelBatch {
	batch, ok := batches[key]
	if !ok {
		batch = &channelBatch{key: key, messageIDs: make(map[int64]struct{})}
		batches[key] = batch
	}
	return batch
}

// flushBatches deletes one de-duplicated message batch per channel, then runs
// the row deletions. Any Telegram failure aborts the run and leaves every row
// and the session state untouched for River to retry.
func (w *UploadCleanupWorker) flushBatches(ctx context.Context, batches map[cleanupChannel]*channelBatch, deleteRows func(*channelBatch, []byte) error) error {
	channels := make([]cleanupChannel, 0, len(batches))
	for channel := range batches {
		channels = append(channels, channel)
	}
	slices.SortFunc(channels, func(a, b cleanupChannel) int {
		return cmp.Or(cmp.Compare(a.userID, b.userID), cmp.Compare(a.channelID, b.channelID))
	})
	for _, key := range channels {
		batch := batches[key]
		messageIDs := make([]int64, 0, len(batch.messageIDs))
		for messageID := range batch.messageIDs {
			messageIDs = append(messageIDs, messageID)
		}
		slices.Sort(messageIDs)
		if err := w.storage.DeleteMessages(ctx, key.userID, key.channelID, messageIDs); err != nil {
			return fmt.Errorf("delete Telegram upload messages for user %d channel %d: %w", key.userID, key.channelID, err)
		}
		encodedReceipts, err := json.Marshal(batch.receipts)
		if err != nil {
			return fmt.Errorf("encode cleaned upload receipts: %w", err)
		}
		if err := deleteRows(batch, encodedReceipts); err != nil {
			return err
		}
	}
	return nil
}

func cleanupReceiptFromRow(partNo int32, leaseToken pgtype.UUID, channelID int64, messageID pgtype.Int8) (cleanupReceipt, bool) {
	token, ok := dbtypes.GoogleUUID(leaseToken)
	if !ok || !messageID.Valid || messageID.Int64 <= 0 {
		return cleanupReceipt{}, false
	}
	return cleanupReceipt{
		PartNo:     partNo,
		LeaseToken: token,
		ChannelID:  channelID,
		MessageID:  messageID.Int64,
	}, true
}
