//go:build integration

package jobs_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/riverqueue/river"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/jobs"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

type batchCleanupStorage struct {
	mu        sync.Mutex
	batches   [][]int64
	deleteErr error
}

func (*batchCleanupStorage) Upload(context.Context, telegramstore.UploadRequest) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, errors.New("not implemented")
}
func (*batchCleanupStorage) OpenRange(context.Context, telegramstore.RangeRequest) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}
func (s *batchCleanupStorage) DeleteMessages(_ context.Context, _ int64, _ int64, ids []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.batches = append(s.batches, append([]int64(nil), ids...))
	return nil
}
func (*batchCleanupStorage) CopyPart(context.Context, int64, int64, int64, int64) (telegramstore.StoredPart, error) {
	return telegramstore.StoredPart{}, errors.New("not implemented")
}
func (*batchCleanupStorage) CreateChannel(context.Context, int64, string) (telegramstore.Channel, error) {
	return telegramstore.Channel{}, errors.New("not implemented")
}
func (*batchCleanupStorage) DeleteChannel(context.Context, int64, int64) error { return nil }

func (s *batchCleanupStorage) messages() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []int64
	for _, batch := range s.batches {
		all = append(all, batch...)
	}
	return all
}
func (s *batchCleanupStorage) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batches)
}

func receiptStateCount(t testing.TB, db *testpostgres.Database) map[sqlcgen.UploadPartReceiptState]int {
	t.Helper()
	rows, err := db.Pool.Query(context.Background(),
		"SELECT state, count(*) FROM upload_part_receipts GROUP BY state")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	counts := make(map[sqlcgen.UploadPartReceiptState]int)
	for rows.Next() {
		var state sqlcgen.UploadPartReceiptState
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			t.Fatal(err)
		}
		counts[state] = count
	}
	return counts
}

func TestCleanupSweepDropsDeadPendingReceiptsWithoutTelegramCalls(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCleanupOwner(t, db.Pool)
	catalog := uploads.NewService(db.Pool)
	session, err := catalog.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "pending.bin", ExpectedSize: 2, PartSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	if _, err := catalog.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		"UPDATE upload_parts SET lease_expires_at = now() - interval '1 minute' WHERE upload_id = $1",
		dbtypes.UUID(uploadID)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		"UPDATE upload_part_receipts SET lease_expires_at = now() - interval '1 minute' WHERE upload_id = $1",
		dbtypes.UUID(uploadID)); err != nil {
		t.Fatal(err)
	}

	storage := &batchCleanupStorage{}
	worker := jobs.NewUploadCleanupWorker(db.Pool, storage)
	if err := worker.Work(ctx, &river.Job[jobs.CleanupSweepArgs]{}); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	if storage.callCount() != 0 {
		t.Fatalf("Telegram delete calls = %d, want 0", storage.callCount())
	}
	if counts := receiptStateCount(t, db); counts[sqlcgen.UploadPartReceiptStatePending] != 0 {
		t.Fatalf("pending receipts = %d, want 0", counts[sqlcgen.UploadPartReceiptStatePending])
	}
	final, err := catalog.Get(ctx, 1001, uploadID)
	if err != nil || final.State != sqlcgen.UploadStateOpen {
		t.Fatalf("session = %#v, %v", final, err)
	}
}

func TestCleanupSweepDeletesPublishedReceiptsForAbortedSession(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCleanupOwner(t, db.Pool)
	catalog := uploads.NewService(db.Pool)
	session, err := catalog.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "aborted-receipt.bin", ExpectedSize: 3, PartSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	claim, err := catalog.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.RecordPartPublication(ctx, uploads.RecordPublicationInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: claim.LeaseToken, MessageID: 55, StoredSize: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Abort(ctx, 1001, uploadID); err != nil {
		t.Fatal(err)
	}

	storage := &batchCleanupStorage{}
	worker := jobs.NewUploadCleanupWorker(db.Pool, storage)
	if err := worker.Work(ctx, &river.Job[jobs.CleanupSweepArgs]{}); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	if got := storage.messages(); len(got) != 1 || got[0] != 55 {
		t.Fatalf("deleted messages = %#v", got)
	}
	var count int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM upload_part_receipts WHERE upload_id=$1",
		dbtypes.UUID(uploadID)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("remaining receipts = %d, want 0", count)
	}
}

func TestCleanupSweepDeletesCondemnedReceiptOnOpenSession(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCleanupOwner(t, db.Pool)
	catalog := uploads.NewService(db.Pool)
	session, err := catalog.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "condemned.bin", ExpectedSize: 3, PartSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	claim, err := catalog.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.RecordPartPublication(ctx, uploads.RecordPublicationInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: claim.LeaseToken, MessageID: 66, StoredSize: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		"UPDATE upload_part_receipts SET state='deleting' WHERE upload_id=$1 AND part_no=1",
		dbtypes.UUID(uploadID)); err != nil {
		t.Fatal(err)
	}

	storage := &batchCleanupStorage{}
	worker := jobs.NewUploadCleanupWorker(db.Pool, storage)
	if err := worker.Work(ctx, &river.Job[jobs.CleanupSweepArgs]{}); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	if got := storage.messages(); len(got) != 1 || got[0] != 66 {
		t.Fatalf("deleted messages = %#v", got)
	}
	final, err := catalog.Get(ctx, 1001, uploadID)
	if err != nil || final.State != sqlcgen.UploadStateOpen {
		t.Fatalf("condemned cleanup changed session = %#v, %v", final, err)
	}
}

func TestCleanupSweepDeletesSupersededReceiptAndKeepsCommittedMessage(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCleanupOwner(t, db.Pool)
	catalog := uploads.NewService(db.Pool)
	blockHashes, checksum := cleanupHash([]byte("xyz"))
	session, err := catalog.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "superseded.bin", ExpectedSize: 3, PartSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)

	// First attempt publishes 78 but never stores the part.
	first, err := catalog.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 3, Checksum: &checksum,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.RecordPartPublication(ctx, uploads.RecordPublicationInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: first.LeaseToken, MessageID: 78, StoredSize: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		"UPDATE upload_parts SET lease_expires_at = now() - interval '1 minute' WHERE upload_id=$1 AND part_no=1",
		dbtypes.UUID(uploadID)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		"UPDATE upload_part_receipts SET lease_expires_at = now() - interval '1 minute' WHERE upload_id=$1 AND part_no=1",
		dbtypes.UUID(uploadID)); err != nil {
		t.Fatal(err)
	}
	// A fresh attempt publishes 77 and commits it.
	second, err := catalog.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 3, Checksum: &checksum,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.StorePart(ctx, uploads.StorePartInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: second.LeaseToken, MessageID: 77, StoredSize: 3,
		Checksum: checksum, BlockHashes: blockHashes,
	}); err != nil {
		t.Fatal(err)
	}

	storage := &batchCleanupStorage{}
	worker := jobs.NewUploadCleanupWorker(db.Pool, storage)
	if err := worker.Work(ctx, &river.Job[jobs.CleanupSweepArgs]{}); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	if got := storage.messages(); len(got) != 1 || got[0] != 78 {
		t.Fatalf("deleted messages = %#v, want only 78", got)
	}
	part, err := catalog.GetPart(ctx, 1001, uploadID, 1)
	if err != nil || part.State != sqlcgen.UploadPartStateStored || part.MessageID.Int64 != 77 {
		t.Fatalf("committed part = %#v, %v", part, err)
	}
}

func TestCleanupSweepCondemnedFailureRetainsAndRetries(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCleanupOwner(t, db.Pool)
	catalog := uploads.NewService(db.Pool)
	session, err := catalog.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "retry-delete.bin", ExpectedSize: 3, PartSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	claim, err := catalog.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.RecordPartPublication(ctx, uploads.RecordPublicationInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: claim.LeaseToken, MessageID: 67, StoredSize: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		"UPDATE upload_part_receipts SET state='deleting' WHERE upload_id=$1 AND part_no=1",
		dbtypes.UUID(uploadID)); err != nil {
		t.Fatal(err)
	}

	storage := &batchCleanupStorage{deleteErr: errors.New("Telegram unavailable")}
	worker := jobs.NewUploadCleanupWorker(db.Pool, storage)
	if err := worker.Work(ctx, &river.Job[jobs.CleanupSweepArgs]{}); err == nil {
		t.Fatal("expected Telegram cleanup failure")
	}
	var count int
	if err := db.Pool.QueryRow(ctx,
		"SELECT count(*) FROM upload_part_receipts WHERE upload_id=$1 AND state='deleting'",
		dbtypes.UUID(uploadID)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("deleting receipts after failure = %d, want 1", count)
	}
	final, err := catalog.Get(ctx, 1001, uploadID)
	if err != nil || final.State != sqlcgen.UploadStateOpen {
		t.Fatalf("failed cleanup changed session = %#v, %v", final, err)
	}

	storage.mu.Lock()
	storage.deleteErr = nil
	storage.mu.Unlock()
	if err := worker.Work(ctx, &river.Job[jobs.CleanupSweepArgs]{}); err != nil {
		t.Fatalf("retry Work() error = %v", err)
	}
	if got := storage.messages(); len(got) != 1 || got[0] != 67 {
		t.Fatalf("retried deleted messages = %#v", got)
	}
}

func TestCleanupSweepGarbageCollectsAdoptedReceiptsAfterCompletion(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCleanupOwner(t, db.Pool)
	catalog := uploads.NewService(db.Pool)
	body := []byte("done")
	session, err := catalog.Create(ctx, uploads.CreateInput{
		UserID: 1001, Name: "committed.bin", ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	blockHashes, checksum := cleanupHash(body)
	claim, err := catalog.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: int64(len(body)), Checksum: &checksum,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.StorePart(ctx, uploads.StorePartInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: claim.LeaseToken, MessageID: 91, StoredSize: int64(len(body)),
		Checksum: checksum, BlockHashes: blockHashes,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Complete(ctx, 1001, uploadID); err != nil {
		t.Fatal(err)
	}

	storage := &batchCleanupStorage{}
	worker := jobs.NewUploadCleanupWorker(db.Pool, storage)
	if err := worker.Work(ctx, &river.Job[jobs.CleanupSweepArgs]{}); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	if storage.callCount() != 0 {
		t.Fatalf("Telegram deletes for committed receipts = %d, want 0", storage.callCount())
	}
	var count int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM upload_part_receipts WHERE upload_id=$1",
		dbtypes.UUID(uploadID)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("adopted receipts after GC = %d, want 0", count)
	}
	var fileParts int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM file_parts WHERE message_id=91").Scan(&fileParts); err != nil {
		t.Fatal(err)
	}
	if fileParts != 1 {
		t.Fatalf("file_parts = %d, want 1", fileParts)
	}
}

func TestCleanupSweepUnifiesPartsAndReceiptsIntoSingleBatch(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCleanupOwner(t, db.Pool)
	catalog := uploads.NewService(db.Pool)
	blockHashes, checksum := cleanupHash([]byte("ab"))
	session, err := catalog.Create(ctx, uploads.CreateInput{UserID: 1001, Name: "unify.bin", ExpectedSize: 2, PartSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	claim, err := catalog.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 2, Checksum: &checksum,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.RecordPartPublication(ctx, uploads.RecordPublicationInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: claim.LeaseToken, MessageID: 77, StoredSize: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.StorePart(ctx, uploads.StorePartInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: claim.LeaseToken, MessageID: 77, StoredSize: 2,
		Checksum: checksum, BlockHashes: blockHashes,
	}); err != nil {
		t.Fatal(err)
	}
	// An older duplicate publish of the same part on a dead lease.
	if _, err := db.Pool.Exec(ctx, `
INSERT INTO upload_part_receipts
    (upload_id, part_no, lease_token, channel_id, plain_size, message_id, stored_size,
     state, lease_expires_at, published_at)
VALUES ($1, 1, gen_random_uuid(), 9001, 2, 78, 2, 'published', now(), now())`,
		dbtypes.UUID(uploadID)); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Abort(ctx, 1001, uploadID); err != nil {
		t.Fatal(err)
	}

	storage := &batchCleanupStorage{}
	worker := jobs.NewUploadCleanupWorker(db.Pool, storage)
	if err := worker.Work(ctx, &river.Job[jobs.CleanupSweepArgs]{}); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	if storage.callCount() != 1 {
		t.Fatalf("DeleteMessages calls = %d, want 1: %#v", storage.callCount(), storage.batches)
	}
	batch := storage.batches[0]
	if len(batch) != 2 || batch[0] != 77 || batch[1] != 78 {
		t.Fatalf("deleted batch = %#v, want [77 78]", batch)
	}
}
