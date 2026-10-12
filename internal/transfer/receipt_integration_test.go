//go:build integration

package transfer_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/tgdrive/teldrive/v2/internal/contentcrypto"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/transfer"
	"github.com/tgdrive/teldrive/v2/internal/treehash"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func checksumHashes(data []byte) ([]byte, string) {
	hasher := treehash.NewBlockHasher()
	_, _ = hasher.Write(data)
	blockHashes := hasher.Sum()
	return blockHashes, treehash.SumToHex(treehash.ComputeTreeHash(blockHashes))
}

// storeFaultCatalog wraps the real uploads service and injects StorePart
// failures to simulate ambiguous commits and hard database errors.
type storeFaultCatalog struct {
	*uploads.Service
	mu              sync.Mutex
	calls           int
	commitThenError bool
	hardError       error
}

func (c *storeFaultCatalog) StorePart(ctx context.Context, in uploads.StorePartInput) (*sqlcgen.UploadPart, error) {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()
	if c.commitThenError && call == 1 {
		part, err := c.Service.StorePart(ctx, in)
		if err != nil {
			return nil, err
		}
		return part, c.hardError
	}
	if c.hardError != nil && !c.commitThenError {
		return nil, c.hardError
	}
	return c.Service.StorePart(ctx, in)
}

func (c *storeFaultCatalog) storeCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func forcePartLeaseExpiry(t testing.TB, db *testpostgres.Database, uploadID uuid.UUID, partNo int32) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, `
UPDATE upload_parts
SET lease_expires_at = now() - interval '1 minute'
WHERE upload_id = $1 AND part_no = $2 AND lease_token IS NOT NULL`,
		dbtypes.UUID(uploadID), partNo); err != nil {
		t.Fatalf("expire part lease: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `
UPDATE upload_part_receipts
SET lease_expires_at = now() - interval '1 minute'
WHERE upload_id = $1 AND part_no = $2`,
		dbtypes.UUID(uploadID), partNo); err != nil {
		t.Fatalf("expire receipt lease: %v", err)
	}
}

func TestAmbiguousStoreCommitReturnsOriginalResultWithoutDelete(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	base := uploads.NewService(db.Pool)
	body := []byte("ambiguous commit")
	session, err := base.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "ambiguous.bin", ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	checksum := checksumFor(body)
	storage := &memoryStorage{}
	catalog := &storeFaultCatalog{Service: base, commitThenError: true, hardError: errors.New("commit outcome unknown")}
	pipeline := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{})

	result, err := pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Checksum: &checksum, Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("UploadPart() after ambiguous commit = %v", err)
	}
	if result.Part.State != sqlcgen.UploadPartStateStored || result.Existing {
		t.Fatalf("result = %#v", result)
	}
	if calls := catalog.storeCalls(); calls != 2 {
		t.Fatalf("StorePart calls = %d, want 2", calls)
	}
	if len(storage.deleted) != 0 {
		t.Fatalf("ambiguous commit deleted messages = %#v", storage.deleted)
	}

	// A whole new request observes the committed part and performs no side effects.
	retry, err := pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Body: panicReader{},
	})
	if err != nil || !retry.Existing {
		t.Fatalf("retry = %#v, %v", retry, err)
	}
	if storage.uploadCount() != 1 {
		t.Fatalf("telegram uploads = %d, want 1", storage.uploadCount())
	}
}

func TestHardStoreErrorLeavesTakeableReceiptAdoptedOnRetry(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	base := uploads.NewService(db.Pool)
	body := []byte("durable receipt")
	session, err := base.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "durable.bin", ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	checksum := checksumFor(body)
	storage := &memoryStorage{}
	catalog := &storeFaultCatalog{Service: base, hardError: errors.New("database unavailable")}
	firstResolver := &fixedResolver{channelID: 9001}
	first := transfer.NewPipeline(catalog, firstResolver, storage, nil, transfer.Config{})

	if _, err := first.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Checksum: &checksum, Body: bytes.NewReader(body),
	}); err == nil {
		t.Fatal("expected hard StorePart error")
	}
	if storage.uploadCount() != 1 || len(storage.deleted) != 0 {
		t.Fatalf("after hard error uploads=%d deleted=%#v", storage.uploadCount(), storage.deleted)
	}
	part, err := base.GetPart(context.Background(), 1001, uploadID, 1)
	if err != nil || part.State == sqlcgen.UploadPartStateStored {
		t.Fatalf("part after hard error = %#v, %v", part, err)
	}
	var receiptState sqlcgen.UploadPartReceiptState
	if err := db.Pool.QueryRow(context.Background(),
		"SELECT state FROM upload_part_receipts WHERE upload_id=$1 AND part_no=1",
		dbtypes.UUID(uploadID)).Scan(&receiptState); err != nil {
		t.Fatalf("read receipt: %v", err)
	}
	if receiptState != sqlcgen.UploadPartReceiptStatePublished {
		t.Fatalf("receipt state = %s, want published", receiptState)
	}

	forcePartLeaseExpiry(t, db, uploadID, 1)
	retryResolver := &fixedResolver{channelID: 9001}
	retry := transfer.NewPipeline(base, retryResolver, storage, nil, transfer.Config{})
	result, err := retry.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Checksum: &checksum, Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("adopting retry = %v", err)
	}
	if result.Existing || result.Part.State != sqlcgen.UploadPartStateStored {
		t.Fatalf("adoption result = %#v", result)
	}
	if storage.uploadCount() != 1 {
		t.Fatalf("telegram uploads after retry = %d, want 1", storage.uploadCount())
	}
	if retryResolver.calls != 0 {
		t.Fatalf("adoption resolved a channel %d times", retryResolver.calls)
	}
	if _, err := base.Complete(context.Background(), 1001, uploadID); err != nil {
		t.Fatalf("complete after adoption: %v", err)
	}
	if _, ok := storage.messages[1]; !ok {
		t.Fatal("adopted Telegram message missing after completion")
	}
}

func TestLateLeaseHolderCannotClobberAdoptedMessage(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	base := uploads.NewService(db.Pool)
	body := []byte("late owner")
	session, err := base.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "late.bin", ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	blockHashes, checksum := checksumHashes(body)

	oldClaim, err := base.ClaimPart(context.Background(), uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: int64(len(body)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := base.RecordPartPublication(context.Background(), uploads.RecordPublicationInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: oldClaim.LeaseToken, MessageID: 30, StoredSize: int64(len(body)),
	}); err != nil {
		t.Fatal(err)
	}
	forcePartLeaseExpiry(t, db, uploadID, 1)

	storage := &memoryStorage{messages: map[int64][]byte{}}
	storage.messages[30] = append([]byte(nil), body...)
	pipeline := transfer.NewPipeline(base, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{})
	newResult, err := pipeline.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Checksum: &checksum, Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("new owner adopt = %v", err)
	}
	if newResult.Part.MessageID.Int64 != 30 || storage.uploadCount() != 0 {
		t.Fatalf("new owner result = %#v uploads=%d", newResult.Part, storage.uploadCount())
	}

	// The old lease holder arrives late: it cannot store, condemn, or delete.
	if _, err := base.StorePart(context.Background(), uploads.StorePartInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: oldClaim.LeaseToken, MessageID: 30, StoredSize: int64(len(body)),
		Checksum: checksum, BlockHashes: blockHashes,
	}); !errors.Is(err, uploads.ErrLeaseLost) {
		t.Fatalf("late StorePart error = %v, want ErrLeaseLost", err)
	}
	condemned, err := base.CondemnPartReceipt(context.Background(), uploads.CondemnPartReceiptInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: oldClaim.LeaseToken,
	})
	if err != nil || condemned != 0 {
		t.Fatalf("late condemn = %d, %v", condemned, err)
	}
	if _, ok := storage.messages[30]; !ok {
		t.Fatal("adopted message was deleted by the late lease holder")
	}
	if _, err := base.Complete(context.Background(), 1001, uploadID); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func TestAdoptEncryptedPartReusesSaltAndCiphertext(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	base := uploads.NewService(db.Pool)
	body := bytes.Repeat([]byte("encrypted-adopt-"), 800)
	version := int32(7)
	session, err := base.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "enc-adopt.bin", ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
		Encryption: true, EncryptionKeyVersion: &version,
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	storage := &memoryStorage{}
	catalog := &storeFaultCatalog{Service: base, hardError: errors.New("database unavailable")}
	first := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage,
		transfer.StaticKeyProvider{7: "same-key-format-as-original"}, transfer.Config{
			Random: bytes.NewReader(bytes.Repeat([]byte{9}, 32)),
		})
	if _, err := first.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Body: bytes.NewReader(body),
	}); err == nil {
		t.Fatal("expected hard StorePart error")
	}
	ciphertext := storage.payload(0)
	forcePartLeaseExpiry(t, db, uploadID, 1)

	retry := transfer.NewPipeline(base, &fixedResolver{channelID: 9001}, storage,
		transfer.StaticKeyProvider{7: "same-key-format-as-original"}, transfer.Config{
			Random: bytes.NewReader(bytes.Repeat([]byte{9}, 32)),
		})
	result, err := retry.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("encrypted adoption = %v", err)
	}
	if !result.Part.Salt.Valid {
		t.Fatal("adopted part has no salt")
	}
	if storage.uploadCount() != 1 || !bytes.Equal(storage.payload(0), ciphertext) {
		t.Fatal("adoption republished or changed ciphertext")
	}
	cipher, err := contentcrypto.NewCipher("same-key-format-as-original", result.Part.Salt.String)
	if err != nil {
		t.Fatal(err)
	}
	decrypted, err := cipher.DecryptData(io.NopCloser(bytes.NewReader(ciphertext)))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(decrypted)
	if err != nil || !bytes.Equal(plain, body) {
		t.Fatalf("adopted decryption = %d bytes, %v", len(plain), err)
	}
}

func TestAdoptChecksumMismatchKeepsMessageTakeable(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	base := uploads.NewService(db.Pool)
	correct := []byte("0123456789")
	wrong := []byte("9876543210")
	session, err := base.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "mismatch.bin", ExpectedSize: int64(len(correct)), PartSize: int64(len(correct)),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	storage := &memoryStorage{}
	catalog := &storeFaultCatalog{Service: base, hardError: errors.New("database unavailable")}
	first := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{})
	if _, err := first.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(correct)), Body: bytes.NewReader(correct),
	}); err == nil {
		t.Fatal("expected hard StorePart error")
	}
	forcePartLeaseExpiry(t, db, uploadID, 1)

	// A retry that streams different bytes with a mismatched checksum fails but
	// must not delete the valid stored message.
	wrongChecksum := checksumFor(wrong)
	retry := transfer.NewPipeline(base, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{})
	if _, err := retry.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(wrong)), Checksum: &wrongChecksum, Body: bytes.NewReader(wrong),
	}); !errors.Is(err, transfer.ErrChecksumMismatch) {
		t.Fatalf("wrong-body adoption error = %v, want ErrChecksumMismatch", err)
	}
	if len(storage.deleted) != 0 || storage.uploadCount() != 1 {
		t.Fatalf("mismatch deleted=%#v uploads=%d", storage.deleted, storage.uploadCount())
	}
	part, err := base.GetPart(context.Background(), 1001, uploadID, 1)
	if err != nil || part.State != sqlcgen.UploadPartStateFailed {
		t.Fatalf("part after mismatch = %#v, %v", part, err)
	}

	// The same correct body retried still adopts the original message.
	forcePartLeaseExpiry(t, db, uploadID, 1)
	if _, err := retry.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(correct)), Body: bytes.NewReader(correct),
	}); err != nil {
		t.Fatalf("correct-body adoption = %v", err)
	}
	if storage.uploadCount() != 1 {
		t.Fatalf("uploads = %d, want 1", storage.uploadCount())
	}
}

func TestOpenEndedUploadAdoptsReceiptAndDerivesSize(t *testing.T) {
	db := testpostgres.New(t)
	seedTransferOwner(t, db.Pool, 1001, 9001)
	base := uploads.NewService(db.Pool)
	body := []byte("open-ended body")
	session, err := base.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "open.bin", ExpectedSize: -1, PartSize: 512,
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, _ := dbtypes.GoogleUUID(session.ID)
	storage := &memoryStorage{}
	catalog := &storeFaultCatalog{Service: base, hardError: errors.New("database unavailable")}
	first := transfer.NewPipeline(catalog, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{})
	if _, err := first.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Body: bytes.NewReader(body),
	}); err == nil {
		t.Fatal("expected hard StorePart error")
	}
	forcePartLeaseExpiry(t, db, uploadID, 1)
	retry := transfer.NewPipeline(base, &fixedResolver{channelID: 9001}, storage, nil, transfer.Config{})
	if _, err := retry.UploadPart(context.Background(), transfer.UploadPartRequest{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Body: bytes.NewReader(body),
	}); err != nil {
		t.Fatalf("open-ended adoption = %v", err)
	}
	file, err := base.Complete(context.Background(), 1001, uploadID)
	if err != nil {
		t.Fatalf("complete open-ended: %v", err)
	}
	if !file.Size.Valid || file.Size.Int64 != int64(len(body)) {
		t.Fatalf("derived size = %#v, want %d", file.Size, len(body))
	}
}
