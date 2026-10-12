//go:build integration

package uploads_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
	"github.com/tgdrive/teldrive/v2/internal/uploads"
)

func expirePartLease(t testing.TB, db *testpostgres.Database, uploadID uuid.UUID, partNo int32) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, `
UPDATE upload_parts
SET lease_expires_at = now() - interval '1 minute'
WHERE upload_id = $1 AND part_no = $2 AND lease_token IS NOT NULL`, dbtypes.UUID(uploadID), partNo); err != nil {
		t.Fatalf("expire part lease: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `
UPDATE upload_part_receipts
SET lease_expires_at = now() - interval '1 minute'
WHERE upload_id = $1 AND part_no = $2`, dbtypes.UUID(uploadID), partNo); err != nil {
		t.Fatalf("expire receipt lease: %v", err)
	}
}

func receiptCount(t testing.TB, db *testpostgres.Database, uploadID uuid.UUID, partNo int32, state sqlcgen.UploadPartReceiptState) int {
	t.Helper()
	var count int
	if err := db.Pool.QueryRow(context.Background(), `
SELECT count(*)
FROM upload_part_receipts
WHERE upload_id = $1 AND part_no = $2 AND state = $3`,
		dbtypes.UUID(uploadID), partNo, state).Scan(&count); err != nil {
		t.Fatalf("count receipts: %v", err)
	}
	return count
}

func newReceiptSession(t testing.TB, db *testpostgres.Database, svc *uploads.Service, size int64) uuid.UUID {
	t.Helper()
	session, err := svc.Create(context.Background(), uploads.CreateInput{
		UserID: 1001, Name: "receipt.bin", ExpectedSize: size, PartSize: size,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	return mustUploadUUID(t, session.ID)
}

func TestClaimPartCreatesPendingReceiptAndRejectsConcurrentClaim(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc := uploads.NewService(db.Pool)
	uploadID := newReceiptSession(t, db, svc, 8)

	claim, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 8,
	})
	if err != nil || claim.Existing {
		t.Fatalf("claim part = %#v, %v", claim, err)
	}
	if got := receiptCount(t, db, uploadID, 1, sqlcgen.UploadPartReceiptStatePending); got != 1 {
		t.Fatalf("pending receipts = %d, want 1", got)
	}
	if _, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 8,
	}); !errors.Is(err, uploads.ErrPartBusy) {
		t.Fatalf("second claim error = %v, want ErrPartBusy", err)
	}
}

func TestPublishedReceiptIsAdoptedWithoutRepublishing(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc := uploads.NewService(db.Pool)
	body := []byte("adopt me")
	uploadID := newReceiptSession(t, db, svc, int64(len(body)))
	blockHashes, checksum := hashMetadata(body)

	first, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	salt := "salt-value"
	if err := svc.RecordPartPublication(ctx, uploads.RecordPublicationInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: first.LeaseToken,
		MessageID: 77, StoredSize: int64(len(body)), Salt: &salt,
	}); err != nil {
		t.Fatalf("record publication: %v", err)
	}
	expirePartLease(t, db, uploadID, 1)

	takeable, err := svc.FindTakeablePart(ctx, uploads.FindTakeableInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("find takeable: %v", err)
	}
	if takeable == nil || takeable.MessageID != 77 || takeable.ChannelID != 9001 || takeable.Salt == nil || *takeable.Salt != salt {
		t.Fatalf("takeable = %#v", takeable)
	}
	wrongSize, err := svc.FindTakeablePart(ctx, uploads.FindTakeableInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: 9,
	})
	if err != nil || wrongSize != nil {
		t.Fatalf("wrong-size takeable = %#v, %v", wrongSize, err)
	}

	second, err := svc.ClaimPartForAdoption(ctx, uploads.ClaimAdoptionInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: int64(len(body)), Checksum: &checksum,
	})
	if err != nil {
		t.Fatalf("claim for adoption: %v", err)
	}
	if second.Adoption == nil || second.Adoption.MessageID != 77 || second.LeaseToken == first.LeaseToken {
		t.Fatalf("adoption claim = %#v", second)
	}
	if second.Part.ChannelID != 9001 {
		t.Fatalf("adopted part channel = %d, want 9001", second.Part.ChannelID)
	}
	if got := receiptCount(t, db, uploadID, 1, sqlcgen.UploadPartReceiptStatePublished); got != 1 {
		t.Fatalf("published receipts after transfer = %d, want 1", got)
	}
	var receiptToken uuid.UUID
	if err := db.Pool.QueryRow(ctx, `
SELECT lease_token FROM upload_part_receipts
WHERE upload_id = $1 AND part_no = 1 AND state = 'published'`,
		dbtypes.UUID(uploadID)).Scan(&receiptToken); err != nil {
		t.Fatalf("read transferred receipt: %v", err)
	}
	if receiptToken != second.LeaseToken {
		t.Fatalf("receipt token = %s, want new lease %s", receiptToken, second.LeaseToken)
	}

	stored, err := svc.StorePart(ctx, uploads.StorePartInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: second.LeaseToken,
		MessageID: 77, StoredSize: int64(len(body)),
		Checksum: checksum, BlockHashes: blockHashes,
	})
	if err != nil {
		t.Fatalf("store adopted part: %v", err)
	}
	if stored.State != sqlcgen.UploadPartStateStored || !stored.Salt.Valid || stored.Salt.String != salt {
		t.Fatalf("stored adopted part = %#v", stored)
	}
	if got := receiptCount(t, db, uploadID, 1, sqlcgen.UploadPartReceiptStateAdopted); got != 1 {
		t.Fatalf("adopted receipts = %d, want 1", got)
	}
}

func TestClaimPartForAdoptionRejectsExpiredSession(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc := uploads.NewService(db.Pool)
	uploadID := newReceiptSession(t, db, svc, 6)
	claim, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.RecordPartPublication(ctx, uploads.RecordPublicationInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: claim.LeaseToken, MessageID: 5, StoredSize: 6,
	}); err != nil {
		t.Fatal(err)
	}
	expirePartLease(t, db, uploadID, 1)
	if _, err := db.Pool.Exec(ctx,
		"UPDATE upload_sessions SET expires_at = now() - interval '1 minute' WHERE id = $1",
		dbtypes.UUID(uploadID)); err != nil {
		t.Fatal(err)
	}
	takeable, err := svc.FindTakeablePart(ctx, uploads.FindTakeableInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: 6,
	})
	if err != nil || takeable != nil {
		t.Fatalf("expired takeable = %#v, %v", takeable, err)
	}
	if _, err := svc.ClaimPartForAdoption(ctx, uploads.ClaimAdoptionInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, PlainSize: 6,
	}); !errors.Is(err, uploads.ErrExpired) {
		t.Fatalf("adoption on expired session error = %v, want ErrExpired", err)
	}
}

func TestFailPartDropsPendingReceiptButKeepsPublishedOne(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc := uploads.NewService(db.Pool)
	uploadID := newReceiptSession(t, db, svc, 5)

	pending, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.FailPart(ctx, uploads.FailPartInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: pending.LeaseToken, ErrorCode: "telegram_upload_failed",
	}); err != nil {
		t.Fatalf("fail pending part: %v", err)
	}
	var receiptCountAll int
	if err := db.Pool.QueryRow(ctx,
		"SELECT count(*) FROM upload_part_receipts WHERE upload_id = $1 AND part_no = 1",
		dbtypes.UUID(uploadID)).Scan(&receiptCountAll); err != nil {
		t.Fatal(err)
	}
	if receiptCountAll != 0 {
		t.Fatalf("receipts after pending failure = %d, want 0", receiptCountAll)
	}

	expirePartLease(t, db, uploadID, 1)
	published, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.RecordPartPublication(ctx, uploads.RecordPublicationInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: published.LeaseToken, MessageID: 9, StoredSize: 5,
	}); err != nil {
		t.Fatal(err)
	}
	// A different failure mode must not destroy the takeable published receipt.
	if _, err := svc.FailPart(ctx, uploads.FailPartInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: published.LeaseToken, ErrorCode: "body_size_mismatch",
	}); err != nil {
		t.Fatalf("fail published part: %v", err)
	}
	if got := receiptCount(t, db, uploadID, 1, sqlcgen.UploadPartReceiptStatePublished); got != 1 {
		t.Fatalf("published receipts after failure = %d, want 1", got)
	}
}

func TestRenewPartExtendsPartAndReceiptLeases(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)
	svc := uploads.NewService(db.Pool)
	uploadID := newReceiptSession(t, db, svc, 4)
	claim, err := svc.ClaimPart(ctx, uploads.ClaimPartInput{
		UserID: 1001, UploadID: uploadID, PartNo: 1, ChannelID: 9001, PlainSize: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	var beforeCount int
	if err := db.Pool.QueryRow(ctx, `
SELECT count(*) FROM upload_parts p
JOIN upload_part_receipts r USING (upload_id, part_no)
WHERE p.upload_id = $1 AND p.part_no = 1
  AND p.lease_token = r.lease_token
  AND p.lease_expires_at = r.lease_expires_at`,
		dbtypes.UUID(uploadID)).Scan(&beforeCount); err != nil {
		t.Fatal(err)
	}
	if beforeCount != 1 {
		t.Fatalf("aligned leases before renew = %d", beforeCount)
	}
	if err := svc.RenewPart(ctx, uploads.RenewPartInput{
		UploadID: uploadID, PartNo: 1, LeaseToken: claim.LeaseToken,
	}); err != nil {
		t.Fatalf("renew: %v", err)
	}
	var renewed int
	if err := db.Pool.QueryRow(ctx, `
SELECT count(*) FROM upload_parts p
JOIN upload_part_receipts r USING (upload_id, part_no)
WHERE p.upload_id = $1 AND p.part_no = 1
  AND p.lease_token = r.lease_token
  AND p.lease_expires_at = r.lease_expires_at
  AND p.lease_expires_at > now()`,
		dbtypes.UUID(uploadID)).Scan(&renewed); err != nil {
		t.Fatal(err)
	}
	if renewed != 1 {
		t.Fatalf("aligned renewed leases = %d, want 1", renewed)
	}
}

func TestCompleteAndAbortRaceHasSingleWinner(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedUploadOwner(t, db.Pool, 1001, 9001)

	for iteration := 0; iteration < 30; iteration++ {
		svc := uploads.NewService(db.Pool)
		body := []byte("race")
		session, err := svc.Create(ctx, uploads.CreateInput{
			UserID: 1001, Name: fmt.Sprintf("race-%d.bin", iteration), ExpectedSize: int64(len(body)), PartSize: int64(len(body)),
		})
		if err != nil {
			t.Fatal(err)
		}
		uploadID := mustUploadUUID(t, session.ID)
		claimAndStore(t, ctx, svc, uploadID, 1, body, int64(200+iteration))

		start := make(chan struct{})
		var completeErr, abortErr error
		var wg2 sync.WaitGroup
		wg2.Add(2)
		go func() {
			defer wg2.Done()
			<-start
			_, completeErr = svc.Complete(ctx, 1001, uploadID)
		}()
		go func() {
			<-start
			_, abortErr = svc.Abort(ctx, 1001, uploadID)
			wg2.Done()
		}()
		close(start)
		wg2.Wait()

		final, err := svc.Get(ctx, 1001, uploadID)
		if err != nil {
			t.Fatalf("get final session: %v", err)
		}
		switch final.State {
		case sqlcgen.UploadStateCompleted:
			if completeErr != nil || !errors.Is(abortErr, uploads.ErrInvalidState) {
				t.Fatalf("iteration %d: complete winner errors complete=%v abort=%v", iteration, completeErr, abortErr)
			}
			if !final.FileID.Valid {
				t.Fatalf("iteration %d: completed without file id", iteration)
			}
		case sqlcgen.UploadStateAborted:
			if abortErr != nil || !errors.Is(completeErr, uploads.ErrInvalidState) {
				t.Fatalf("iteration %d: abort winner errors complete=%v abort=%v", iteration, completeErr, abortErr)
			}
		default:
			t.Fatalf("iteration %d: non-terminal state %q", iteration, final.State)
		}
	}
}
