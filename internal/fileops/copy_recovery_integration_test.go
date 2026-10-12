//go:build integration

package fileops

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tgdrive/teldrive/v2/internal/catalog"
	"github.com/tgdrive/teldrive/v2/internal/channels"
	"github.com/tgdrive/teldrive/v2/internal/db/sqlcgen"
	"github.com/tgdrive/teldrive/v2/internal/dbtypes"
	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
	testpostgres "github.com/tgdrive/teldrive/v2/internal/testutil/postgres"
)

// recoveryStorage wraps the in-memory Telegram storage with per-source copy
// counters and a one-shot gate, so tests can pause a copy between the confirmed
// receipt and the catalog publish.
type recoveryStorage struct {
	*fileStorage
	copyCountBySource map[int64]int
	mu                sync.Mutex
	gate              <-chan struct{}
	gateSource        int64
	gateUsed          bool
	gateIgnoreCancel  bool
	reached           chan struct{}
	reachedOnce       sync.Once
}

func newRecoveryStorage(messages map[int64][]byte, nextID int64) *recoveryStorage {
	return &recoveryStorage{
		fileStorage:       &fileStorage{messages: messages, nextID: nextID},
		copyCountBySource: make(map[int64]int),
		reached:           make(chan struct{}),
	}
}

func (s *recoveryStorage) CopyPart(ctx context.Context, userID, sourceChannelID, sourceMessageID, destinationChannelID int64) (telegramstore.StoredPart, error) {
	s.mu.Lock()
	shouldBlock := s.gate != nil && sourceMessageID == s.gateSource && !s.gateUsed
	if shouldBlock {
		s.gateUsed = true
	}
	s.copyCountBySource[sourceMessageID]++
	s.mu.Unlock()
	if shouldBlock {
		s.reachedOnce.Do(func() { close(s.reached) })
		if s.gateIgnoreCancel {
			<-s.gate
		} else {
			select {
			case <-s.gate:
			case <-ctx.Done():
				return telegramstore.StoredPart{}, ctx.Err()
			}
		}
	}
	return s.fileStorage.CopyPart(ctx, userID, sourceChannelID, sourceMessageID, destinationChannelID)
}

func (s *recoveryStorage) copyCount(messageID int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.copyCountBySource[messageID]
}

func newCopyTestService(t *testing.T, db *testpostgres.Database, storage telegramstore.Storage) *Service {
	t.Helper()
	service, err := NewService(db.Pool, catalog.NewService(db.Pool, nil), channels.NewService(db.Pool, nil, channels.Config{PartLimit: 2000}), storage)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func seedCopyTestUser(t *testing.T, db *testpostgres.Database) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Pool.Exec(ctx, "INSERT INTO users (user_id) VALUES (1001)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, "INSERT INTO channels (channel_id,user_id,name,selected) VALUES (9001,1001,'storage',true)"); err != nil {
		t.Fatal(err)
	}
}

func insertMultipartSource(t *testing.T, db *testpostgres.Database, name string, size int64, messageIDs ...int64) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	id := uuid.New()
	if _, err := db.Pool.Exec(ctx, `
INSERT INTO files (id,user_id,name,kind,mime_type,size,encryption,status,mod_time)
VALUES ($1,1001,$2,'file','application/octet-stream',$3,false,'active',now())`, id, name, size); err != nil {
		t.Fatal(err)
	}
	for index, messageID := range messageIDs {
		if _, err := db.Pool.Exec(ctx, `
INSERT INTO file_parts (file_id,part_no,channel_id,message_id,plain_size,stored_size,checksum,block_hashes)
VALUES ($1,$2,9001,$3,4,4,repeat('a',64),decode(repeat('ab',32),'hex'))`, id, index+1, messageID); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func activeFileExists(t *testing.T, db *testpostgres.Database, name string, parentID *uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := db.Pool.QueryRow(context.Background(), `
SELECT id FROM files
WHERE user_id=1001 AND name=$1 AND status='active'
  AND parent_id IS NOT DISTINCT FROM $2`, name, parentID).Scan(&id)
	if err != nil {
		t.Fatalf("active file %q: %v", name, err)
	}
	return id
}

func waitForCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func copyOperationState(t *testing.T, db *testpostgres.Database, opID uuid.UUID) sqlcgen.CopyOperationState {
	t.Helper()
	var state sqlcgen.CopyOperationState
	if err := db.Pool.QueryRow(context.Background(),
		"SELECT state FROM copy_operations WHERE id=$1", opID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func latestCopyOperationID(t *testing.T, db *testpostgres.Database) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := db.Pool.QueryRow(context.Background(),
		"SELECT id FROM copy_operations WHERE user_id=1001 ORDER BY created_at DESC, id DESC LIMIT 1").Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func mustGoogleUUID(t *testing.T, value pgtype.UUID) uuid.UUID {
	t.Helper()
	id, ok := dbtypes.GoogleUUID(value)
	if !ok {
		t.Fatal("invalid uuid value")
	}
	return id
}

func strPtr(value string) *string {
	return &value
}

// planCopyOperation drives the durable Copy preparation up to (but not
// including) Telegram copies, giving tests a stable operation to manipulate.
func planCopyOperation(t *testing.T, service *Service, ctx context.Context, in CopyInput) *sqlcgen.CopyOperation {
	t.Helper()
	if in.ConflictPolicy == "" {
		in.ConflictPolicy = sqlcgen.NameConflictPolicyFail
	}
	nodes, err := service.loadTree(ctx, in.UserID, in.FileID)
	if err != nil {
		t.Fatal(err)
	}
	idMap := make(map[uuid.UUID]uuid.UUID, len(nodes))
	for _, node := range nodes {
		oldID, ok := dbtypes.GoogleUUID(node.File.ID)
		if !ok {
			t.Fatal(err)
		}
		idMap[oldID] = uuid.New()
	}
	var sourceFileIDs []uuid.UUID
	for _, node := range nodes {
		if node.File.Kind != sqlcgen.FileKindFile {
			continue
		}
		oldID, _ := dbtypes.GoogleUUID(node.File.ID)
		sourceFileIDs = append(sourceFileIDs, oldID)
	}
	parts, err := service.queries.ListFilePartsByFileIDs(ctx, pgUUIDs(sourceFileIDs))
	if err != nil {
		t.Fatal(err)
	}
	destinations, err := service.channels.ResolveMany(ctx, in.UserID, len(parts))
	if err != nil {
		t.Fatal(err)
	}
	op, err := service.createCopyOperation(ctx, in, nodes, parts, destinations, idMap)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func confirmReceipt(t *testing.T, db *testpostgres.Database, opID pgtype.UUID, sourceFileID pgtype.UUID, partNo int32, destMessageID int64) {
	t.Helper()
	if _, err := db.Pool.Exec(context.Background(), `
UPDATE copy_operation_parts
SET dest_message_id=$4, copied_at=now()
WHERE operation_id=$1 AND source_file_id=$2 AND part_no=$3`,
		opID, sourceFileID, partNo, destMessageID); err != nil {
		t.Fatal(err)
	}
}

func TestCopyEmptyFolderAndChunklessFile(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCopyTestUser(t, db)
	storage := newRecoveryStorage(map[int64][]byte{}, 0)
	service := newCopyTestService(t, db, storage)

	emptyID := insertFolder(t, db, nil, "empty-dir")
	zeroID := uuid.New()
	if _, err := db.Pool.Exec(ctx, `
INSERT INTO files (id,user_id,name,kind,mime_type,size,encryption,status,mod_time)
VALUES ($1,1001,'zero.bin','file','application/octet-stream',0,false,'active',now())`, zeroID); err != nil {
		t.Fatal(err)
	}

	copiedEmpty, err := service.Copy(ctx, CopyInput{UserID: 1001, FileID: emptyID, Name: strPtr("empty-dir-copy")})
	if err != nil {
		t.Fatalf("Copy(empty folder) error = %v", err)
	}
	copiedZero, err := service.Copy(ctx, CopyInput{UserID: 1001, FileID: zeroID, Name: strPtr("zero-copy.bin")})
	if err != nil {
		t.Fatalf("Copy(chunkless file) error = %v", err)
	}
	if copiedEmpty.Kind != sqlcgen.FileKindFolder || copiedZero.Kind != sqlcgen.FileKindFile {
		t.Fatalf("unexpected kinds: %s %s", copiedEmpty.Kind, copiedZero.Kind)
	}
	if copiedZero.Size.Int64 != 0 {
		t.Fatalf("zero file size = %d", copiedZero.Size.Int64)
	}
	var partRows int
	if err := db.Pool.QueryRow(ctx, `
SELECT count(*) FROM copy_operation_parts`).Scan(&partRows); err != nil {
		t.Fatal(err)
	}
	if partRows != 0 {
		t.Fatalf("copy operation part rows = %d, want 0", partRows)
	}
	zeroCopyID := mustGoogleUUID(t, copiedZero.ID)
	var publishedParts int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM file_parts WHERE file_id=$1", zeroCopyID).Scan(&publishedParts); err != nil {
		t.Fatal(err)
	}
	if publishedParts != 0 {
		t.Fatalf("zero-byte copy parts = %d, want 0", publishedParts)
	}
	if len(storage.copyCountBySource) != 0 {
		t.Fatalf("expected no Telegram copies for chunkless tree, got %#v", storage.copyCountBySource)
	}
}

func TestCopyResumeSkipsConfirmedReceiptsAndPublishes(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCopyTestUser(t, db)
	sourceID := insertMultipartSource(t, db, "two-part.bin", 8, 41, 42)
	storage := newRecoveryStorage(map[int64][]byte{41: []byte("part"), 42: []byte("part")}, 200)
	service := newCopyTestService(t, db, storage)

	name := "resumed-copy.bin"
	copied, err := service.Copy(ctx, CopyInput{UserID: 1001, FileID: sourceID, Name: &name})
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	opID := latestCopyOperationID(t, db)
	if state := copyOperationState(t, db, opID); state != sqlcgen.CopyOperationStatePublished {
		t.Fatalf("op state = %s", state)
	}

	// Simulate a restart that only knows the operation identity: re-drive the
	// same op as if its in-memory state were still unfinished.
	staleOp, err := service.queries.GetCopyOperation(ctx, dbtypes.UUID(opID))
	if err != nil {
		t.Fatal(err)
	}
	staleOp.State = sqlcgen.CopyOperationStatePartsPending
	staleOp.ResultRootID = pgtype.UUID{}

	file, _, err := service.driveCopyOperation(ctx, staleOp)
	if err != nil {
		t.Fatalf("driveCopyOperation(published) error = %v", err)
	}
	if mustGoogleUUID(t, file.ID) != mustGoogleUUID(t, copied.ID) {
		t.Fatalf("resumed drive returned a different root")
	}
	if storage.copyCount(41) != 1 || storage.copyCount(42) != 1 {
		t.Fatalf("confirmed parts re-copied: %#v", storage.copyCountBySource)
	}
	if got := storage.message(201); got == nil {
		t.Fatalf("published receipt message deleted by idempotent drive")
	}
}

func TestCopyCompensationPreservesReferencedAndUnreferencedSplit(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCopyTestUser(t, db)

	// An existing catalog file already references destination message 301:
	// compensation must never delete it even if this operation holds a receipt.
	insertStoredFile(t, db, "referencer.bin", 301)
	storage := newRecoveryStorage(map[int64][]byte{
		41: []byte("part"), 42: []byte("part"), 43: []byte("part"), 301: []byte("referenced"),
	}, 300)
	storage.fileStorage.failCopyAt = 3
	service := newCopyTestService(t, db, storage)
	sourceID := insertMultipartSource(t, db, "three-part.bin", 12, 41, 42, 43)

	name := "protected-copy.bin"
	if _, err := service.Copy(ctx, CopyInput{UserID: 1001, FileID: sourceID, Name: &name}); err == nil {
		t.Fatal("expected third Telegram copy to fail")
	}
	// Receipt 301 collides with a pre-existing catalog reference (the exact
	// shape of an ambiguous-commit compensation): it must survive even though
	// this operation's copy created the message. The mock reuses the map slot.
	if got := storage.message(301); got == nil {
		t.Fatalf("catalog-referenced message deleted despite protection")
	}
	if got := storage.message(302); got != nil {
		t.Fatalf("unreferenced receipt 302 survived compensation: %q", got)
	}
	if calls := storage.deleteCallsSnapshot(); len(calls) != 1 || len(calls[0]) != 1 || calls[0][0] != 302 {
		t.Fatalf("delete calls = %#v, want only [302]", calls)
	}
	var referencerParts int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM file_parts WHERE channel_id=9001 AND message_id=301").Scan(&referencerParts); err != nil {
		t.Fatal(err)
	}
	if referencerParts != 1 {
		t.Fatalf("catalog reference for message 301 lost")
	}
	if state := copyOperationState(t, db, latestCopyOperationID(t, db)); state != sqlcgen.CopyOperationStateAborted {
		t.Fatalf("op state = %s, want aborted", state)
	}
	var count int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM files WHERE user_id=1001 AND name='protected-copy.bin'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("copy catalog row published after failed copy: %d", count)
	}
}

func TestCopyAbortKeepsMessagesClaimedByLaterOperation(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCopyTestUser(t, db)

	// Two one-part sources. Each unfinished operation holds a receipt for the
	// same destination message id, modelling Telegram reusing the id after an
	// earlier deletion.
	olderSource := insertMultipartSource(t, db, "older.bin", 4, 71)
	newerSource := insertMultipartSource(t, db, "newer.bin", 4, 81)
	storage := newRecoveryStorage(map[int64][]byte{
		71: []byte("part"), 81: []byte("part"), 301: []byte("claimed"),
	}, 0)
	service := newCopyTestService(t, db, storage)

	older := planCopyOperation(t, service, ctx, CopyInput{
		UserID: 1001, FileID: olderSource, Name: strPtr("older-copy.bin"), ConflictPolicy: sqlcgen.NameConflictPolicyFail,
	})
	newer := planCopyOperation(t, service, ctx, CopyInput{
		UserID: 1001, FileID: newerSource, Name: strPtr("newer-copy.bin"), ConflictPolicy: sqlcgen.NameConflictPolicyFail,
	})
	olderParts := mustListParts(t, service, ctx, older.ID)
	newerParts := mustListParts(t, service, ctx, newer.ID)
	confirmReceipt(t, db, older.ID, olderParts[0].SourceFileID, olderParts[0].PartNo, 301)
	confirmReceipt(t, db, newer.ID, newerParts[0].SourceFileID, newerParts[0].PartNo, 301)

	// Force the older operation into the fail-policy conflict path so it aborts.
	insertFolder(t, db, nil, "older-copy.bin")
	if _, _, err := service.driveCopyOperation(ctx, older); !errors.Is(err, catalog.ErrConflict) {
		t.Fatalf("older drive error = %v, want catalog.ErrConflict", err)
	}
	if got := storage.message(301); !bytes.Equal(got, []byte("claimed")) {
		t.Fatalf("message claimed by the newer live operation was deleted: %q", got)
	}
	if calls := storage.deleteCallsSnapshot(); len(calls) != 0 {
		t.Fatalf("abort deleted messages claimed elsewhere: %#v", calls)
	}
	olderID := mustGoogleUUID(t, older.ID)
	newerID := mustGoogleUUID(t, newer.ID)
	if state := copyOperationState(t, db, olderID); state != sqlcgen.CopyOperationStateAborted {
		t.Fatalf("older op state = %s, want aborted", state)
	}
	if state := copyOperationState(t, db, newerID); state != sqlcgen.CopyOperationStatePartsPending {
		t.Fatalf("newer op state = %s, want parts_pending", state)
	}

	// The newer operation later publishes successfully using its receipt.
	file, _, err := service.driveCopyOperation(ctx, newer)
	if err != nil {
		t.Fatalf("newer drive error = %v", err)
	}
	parts, err := catalog.NewService(db.Pool, nil).Parts(ctx, 1001, mustGoogleUUID(t, file.ID))
	if err != nil || len(parts) != 1 || parts[0].MessageID != 301 {
		t.Fatalf("newer parts = %#v, %v", parts, err)
	}
	if got := storage.message(301); !bytes.Equal(got, []byte("claimed")) {
		t.Fatalf("published claimed message payload changed: %q", got)
	}
}

func TestCopyRecoveryConvergesRenameAndReplaceConflicts(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCopyTestUser(t, db)
	storage := newRecoveryStorage(map[int64][]byte{}, 0)
	service := newCopyTestService(t, db, storage)

	sourceA := insertFolder(t, db, nil, "source-a")
	opA := planCopyOperation(t, service, ctx, CopyInput{
		UserID: 1001, FileID: sourceA, Name: strPtr("dup"), ConflictPolicy: sqlcgen.NameConflictPolicyRename,
	})
	// Concurrent creation landed the target name while the copy was paused.
	concurrent := insertFolder(t, db, nil, "dup")

	fileA, _, err := service.driveCopyOperation(ctx, opA)
	if err != nil {
		t.Fatalf("recovered rename copy error = %v", err)
	}
	if fileA.Name != "dup (1)" {
		t.Fatalf("recovered copy name = %q, want dup (1)", fileA.Name)
	}
	assertFileStatus(t, db, concurrent, sqlcgen.FileStatusActive)
	assertActiveNameCount(t, db, "dup", 1)
	assertActiveNameCount(t, db, "dup (1)", 1)
	if state := copyOperationState(t, db, mustGoogleUUID(t, opA.ID)); state != sqlcgen.CopyOperationStatePublished {
		t.Fatalf("opA state = %s", state)
	}

	// Replace on a stale op converges on the same conflict ownership and marks
	// the concurrent tree for purge instead of producing two active roots.
	sourceB := insertFolder(t, db, nil, "source-b")
	opB := planCopyOperation(t, service, ctx, CopyInput{
		UserID: 1001, FileID: sourceB, Name: strPtr("dup"), ConflictPolicy: sqlcgen.NameConflictPolicyReplace,
	})
	fileB, replaced, err := service.driveCopyOperation(ctx, opB)
	if err != nil {
		t.Fatalf("recovered replace copy error = %v", err)
	}
	if fileB.Name != "dup" || fileB.Status != sqlcgen.FileStatusActive {
		t.Fatalf("replace copy = %#v", fileB)
	}
	if len(replaced) == 0 {
		t.Fatal("replace recovery recorded no replaced ids")
	}
	assertFileStatus(t, db, concurrent, sqlcgen.FileStatusDeletionPending)
	assertActiveNameCount(t, db, "dup", 1)
}

func TestCopyRecoveryContinuesFromSnapshotAfterSourcePurge(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCopyTestUser(t, db)
	sourceID := insertMultipartSource(t, db, "purged-source.bin", 8, 41, 42)
	storage := newRecoveryStorage(map[int64][]byte{41: []byte("part"), 42: []byte("part")}, 200)
	gate := make(chan struct{})
	storage.gate = gate
	storage.gateSource = 42
	storage.gateIgnoreCancel = true
	service := newCopyTestService(t, db, storage)

	name := "restart-copy.bin"
	copyResult := make(chan copyOutcome, 1)
	go func() {
		file, err := service.Copy(ctx, CopyInput{UserID: 1001, FileID: sourceID, Name: &name})
		copyResult <- copyOutcome{file: file, err: err}
	}()
	<-storage.reached

	// Part 41 has a confirmed receipt; wait for it durably.
	waitForCondition(t, "confirmed part receipt", func() bool {
		var confirmed int
		_ = db.Pool.QueryRow(ctx, `
SELECT count(*) FROM copy_operation_parts p
JOIN copy_operations o ON o.id = p.operation_id
WHERE o.user_id=1001 AND p.source_message_id=41 AND p.dest_message_id IS NOT NULL`).Scan(&confirmed)
		return confirmed == 1
	})
	opID := latestCopyOperationID(t, db)

	// Simulate a hard restart with the request still hanging: purge the source
	// CATALOG tree while copying. The Telegram source messages stay available.
	if _, err := db.Pool.Exec(ctx, "DELETE FROM file_parts WHERE file_id=$1", sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, "DELETE FROM files WHERE id=$1", sourceID); err != nil {
		t.Fatal(err)
	}

	recovered, err := service.RecoverPendingCopies(ctx, time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("RecoverPendingCopies() error = %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered %d operations, want 1", recovered)
	}
	if state := copyOperationState(t, db, opID); state != sqlcgen.CopyOperationStatePublished {
		t.Fatalf("recovered op state = %s", state)
	}
	if storage.copyCount(41) != 1 {
		t.Fatalf("confirmed part copied again: %d", storage.copyCount(41))
	}
	if storage.copyCount(42) < 1 {
		t.Fatalf("missing part was never copied by recovery")
	}
	rootID := activeFileExists(t, db, "restart-copy.bin", nil)
	parts, err := catalog.NewService(db.Pool, nil).Parts(ctx, 1001, rootID)
	if err != nil || len(parts) != 2 {
		t.Fatalf("recovered parts = %#v, %v", parts, err)
	}
	for _, part := range parts {
		if got := storage.message(part.MessageID); !bytes.Equal(got, []byte("part")) {
			t.Fatalf("recovered part message %d payload = %q", part.MessageID, got)
		}
	}
	var destChannels []int64
	rows, err := db.Pool.Query(ctx, "SELECT dest_channel_id FROM copy_operation_parts WHERE operation_id=$1 ORDER BY part_no", dbtypes.UUID(opID))
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var channelID int64
		if err := rows.Scan(&channelID); err != nil {
			t.Fatal(err)
		}
		destChannels = append(destChannels, channelID)
	}
	rows.Close()
	if len(destChannels) != 2 || destChannels[0] != 9001 || destChannels[1] != 9001 {
		t.Fatalf("pinned destination channels changed: %v", destChannels)
	}

	// Release the paused call: it must observe the existing publish and return
	// the same root instead of duplicating or deleting anything.
	close(gate)
	select {
	case outcome := <-copyResult:
		if outcome.err != nil {
			t.Fatalf("paused copy returned error after recovery publish: %v", outcome.err)
		}
		if outcome.file == nil || mustGoogleUUID(t, outcome.file.ID) != rootID {
			t.Fatalf("paused copy returned different root: %#v", outcome.file)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("paused copy did not converge after recovery")
	}
}

func TestCopyFailedRemoteCleanupStaysPendingAndRecoveryPublishes(t *testing.T) {
	db := testpostgres.New(t)
	ctx := context.Background()
	seedCopyTestUser(t, db)
	sourceID := insertMultipartSource(t, db, "two-part.bin", 8, 41, 42)
	storage := newRecoveryStorage(map[int64][]byte{41: []byte("part"), 42: []byte("part")}, 200)
	storage.fileStorage.failCopyAt = 2
	storage.fileStorage.deleteErr = errors.New("telegram unavailable")
	service := newCopyTestService(t, db, storage)

	name := "pending-copy.bin"
	if _, err := service.Copy(ctx, CopyInput{UserID: 1001, FileID: sourceID, Name: &name}); err == nil {
		t.Fatal("expected copy failure")
	}
	opID := latestCopyOperationID(t, db)
	if state := copyOperationState(t, db, opID); state != sqlcgen.CopyOperationStatePartsPending {
		t.Fatalf("op state after failed cleanup = %s, want parts_pending", state)
	}
	var lastError pgtype.Text
	if err := db.Pool.QueryRow(ctx, "SELECT last_error FROM copy_operations WHERE id=$1", dbtypes.UUID(opID)).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if !lastError.Valid || lastError.String == "" {
		t.Fatal("expected observable last_error on pending operation")
	}

	// Telegram recovers. The next sweep finishes the copy along its receipts;
	// its earlier unpublished message survives and becomes the published part.
	storage.mu.Lock()
	storage.fileStorage.failCopyAt = 0
	storage.fileStorage.deleteErr = nil
	storage.mu.Unlock()
	recovered, err := service.RecoverPendingCopies(ctx, time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("RecoverPendingCopies() error = %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}
	rootID := activeFileExists(t, db, "pending-copy.bin", nil)
	parts, err := catalog.NewService(db.Pool, nil).Parts(ctx, 1001, rootID)
	if err != nil || len(parts) != 2 {
		t.Fatalf("recovered parts = %#v, %v", parts, err)
	}
	if got := storage.message(201); !bytes.Equal(got, []byte("part")) {
		t.Fatalf("first receipt message was deleted instead of becoming published: %q", got)
	}
	if state := copyOperationState(t, db, opID); state != sqlcgen.CopyOperationStatePublished {
		t.Fatalf("op state after recovery = %s, want published", state)
	}
}

func mustListParts(t *testing.T, service *Service, ctx context.Context, opID pgtype.UUID) []*sqlcgen.CopyOperationPart {
	t.Helper()
	parts, err := service.queries.ListCopyOperationParts(ctx, opID)
	if err != nil {
		t.Fatal(err)
	}
	return parts
}

type copyOutcome struct {
	file *sqlcgen.File
	err  error
}
