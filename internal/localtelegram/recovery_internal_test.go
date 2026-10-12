package localtelegram

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/tgdrive/teldrive/v2/internal/telegramstore"
)

type baseline struct {
	server  *Server
	storage telegramstore.Storage
	channel int64
	payload []byte
	stored  telegramstore.StoredPart
}

func newBaseline(t *testing.T, root string) baseline {
	t.Helper()
	ctx := context.Background()
	server, err := Open(root)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	runner, err := NewRunner(server)
	if err != nil {
		t.Fatal(err)
	}
	storage := telegramstore.NewGotdStorage(runner, nil)
	channel, err := storage.CreateChannel(ctx, 1001, "recovery")
	if err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	payload := bytes.Repeat([]byte("abcdef0123456789"), 512)
	stored, err := storage.Upload(ctx, telegramstore.UploadRequest{
		UserID: 1001, ChannelID: channel.ID, Name: "payload.bin",
		Reader: bytes.NewReader(payload), Size: int64(len(payload)), Threads: 2,
	})
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	return baseline{server: server, storage: storage, channel: channel.ID, payload: payload, stored: stored}
}

func reopen(t *testing.T, root string) *Server {
	t.Helper()
	server, err := Open(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return server
}

func writeIntent(t *testing.T, server *Server, commit pendingCommit) {
	t.Helper()
	payload, err := json.MarshalIndent(commit, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(server.pendingDir, fmt.Sprintf("%d.json", commit.Document.ID)), payload, 0o600); err != nil {
		t.Fatal(err)
	}
}

func sha256Hex(payload []byte) string {
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:])
}

func newCommit(server *Server, channelID int64, documentID int64, messageID int, uploadFileID int64, parts int, payload []byte) pendingCommit {
	return pendingCommit{
		Version:      stateVersion,
		ChannelID:    channelID,
		MessageID:    messageID,
		UploadFileID: uploadFileID,
		UploadParts:  parts,
		Document: documentRecord{
			ID:            documentID,
			AccessHash:    documentID + 1_000_000,
			FileReference: []byte(sha256Hex(payload)),
			MimeType:      "application/octet-stream",
			Size:          int64(len(payload)),
			DCID:          1,
			FileName:      "replayed.bin",
			CreatedAt:     1700000000,
		},
		CreatedAt: 1700000000,
	}
}

func writeBlob(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeParts(t *testing.T, server *Server, uploadFileID int64, payload []byte, partSize int) int {
	t.Helper()
	dir := filepath.Join(server.uploadsDir, fmt.Sprintf("%d", uploadFileID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	parts := 0
	for offset := 0; offset < len(payload); offset += partSize {
		end := min(offset+partSize, len(payload))
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%08d.part", parts)), payload[offset:end], 0o600); err != nil {
			t.Fatal(err)
		}
		parts++
	}
	return parts
}

func downloadPart(t *testing.T, storage telegramstore.Storage, channelID int64, messageID int64, want []byte) {
	t.Helper()
	reader, err := storage.OpenRange(context.Background(), telegramstore.RangeRequest{
		UserID: 1001, ChannelID: channelID, MessageID: messageID, Offset: 0, Length: int64(len(want)),
	})
	if err != nil {
		t.Fatalf("OpenRange(%d) error = %v", messageID, err)
	}
	got, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read message %d: %v / %v", messageID, readErr, closeErr)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("message %d content mismatch: got %d bytes, want %d", messageID, len(got), len(want))
	}
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// A published blob with a durable intent but no committed index is finished
// on restart, and reopening again neither republishes nor loses it.
func TestRecoverReplaysPublishedUnindexedDocument(t *testing.T) {
	root := t.TempDir()
	base := newBaseline(t, root)

	documentID := base.server.state.NextDocumentID
	messageID := base.server.state.NextMessageID
	extra := bytes.Repeat([]byte("z"), 4096)
	writeBlob(t, filepath.Join(base.server.documentsDir, fmt.Sprintf("%d.bin", documentID)), extra)
	writeIntent(t, base.server, newCommit(base.server, base.channel, documentID, messageID, 777, 1, extra))

	server := reopen(t, root)
	if report := server.RecoveryReport(); report.ReplayedCommits != 1 || len(report.Quarantined) != 0 {
		t.Fatalf("recovery report = %#v", report)
	}
	if server.state.NextDocumentID != documentID+1 || server.state.NextMessageID != messageID+1 {
		t.Fatalf("counters not pinned: doc=%d msg=%d", server.state.NextDocumentID, server.state.NextMessageID)
	}
	runner, err := NewRunner(server)
	if err != nil {
		t.Fatal(err)
	}
	storage := telegramstore.NewGotdStorage(runner, nil)
	downloadPart(t, storage, base.channel, int64(messageID), extra)
	downloadPart(t, storage, base.stored.ChannelID, base.stored.MessageID, base.payload)

	// Second reopen must converge to the same view without churning anything.
	again := reopen(t, root)
	if report := again.RecoveryReport(); report.ReplayedCommits != 0 || len(report.Quarantined) != 0 || len(report.DanglingReferences) != 0 {
		t.Fatalf("second reopen report = %#v", report)
	}
	if len(dirEntries(t, again.pendingDir)) != 0 {
		t.Fatalf("pending dir not empty: %v", dirEntries(t, again.pendingDir))
	}
	runner2, _ := NewRunner(again)
	downloadPart(t, telegramstore.NewGotdStorage(runner2, nil), base.channel, int64(messageID), extra)
}

// A staged blob that did not reach the documents directory is installed from
// its intent; when neither exists, the original upload parts rebuild it.
func TestRecoverReplaysFromStagedBlobAndFromParts(t *testing.T) {
	t.Run("staged blob", func(t *testing.T) {
		root := t.TempDir()
		base := newBaseline(t, root)
		documentID := base.server.state.NextDocumentID
		messageID := base.server.state.NextMessageID
		extra := []byte("staged but not installed")
		commit := newCommit(base.server, base.channel, documentID, messageID, 778, 1, extra)
		writeBlob(t, filepath.Join(base.server.pendingDir, stagingBlobName(documentID)), extra)
		writeIntent(t, base.server, commit)

		server := reopen(t, root)
		if report := server.RecoveryReport(); report.ReplayedCommits != 1 {
			t.Fatalf("recovery report = %#v", report)
		}
		runner, _ := NewRunner(server)
		downloadPart(t, telegramstore.NewGotdStorage(runner, nil), base.channel, int64(messageID), extra)
	})

	t.Run("rebuild from parts", func(t *testing.T) {
		root := t.TempDir()
		base := newBaseline(t, root)
		documentID := base.server.state.NextDocumentID
		messageID := base.server.state.NextMessageID
		extra := bytes.Repeat([]byte("part"), 700)
		const uploadFileID int64 = 888
		parts := writeParts(t, base.server, uploadFileID, extra, 512)
		writeIntent(t, base.server, newCommit(base.server, base.channel, documentID, messageID, uploadFileID, parts, extra))

		server := reopen(t, root)
		if report := server.RecoveryReport(); report.ReplayedCommits != 1 {
			t.Fatalf("recovery report = %#v", report)
		}
		if entries := dirEntries(t, server.uploadsDir); len(entries) != 0 {
			t.Fatalf("uploads dir not drained: %v", entries)
		}
		runner, _ := NewRunner(server)
		downloadPart(t, telegramstore.NewGotdStorage(runner, nil), base.channel, int64(messageID), extra)
	})
}

// When the intent cannot be completed (channel never committed or parts are
// unusable), nothing is invented: the intent and bytes are quarantined.
func TestRecoverQuarantinesUnconfirmedPublish(t *testing.T) {
	t.Run("missing channel", func(t *testing.T) {
		root := t.TempDir()
		base := newBaseline(t, root)
		documentID := base.server.state.NextDocumentID
		extra := []byte("never confirmed")
		const uploadFileID int64 = 999
		writeParts(t, base.server, uploadFileID, extra, 512)
		writeBlob(t, filepath.Join(base.server.pendingDir, stagingBlobName(documentID)), extra)
		writeIntent(t, base.server, newCommit(base.server, 555, documentID, base.server.state.NextMessageID, uploadFileID, 1, extra))

		server := reopen(t, root)
		report := server.RecoveryReport()
		if report.ReplayedCommits != 0 || len(report.Quarantined) != 2 || report.DiscardedUploads != 1 {
			t.Fatalf("recovery report = %#v", report)
		}
		if _, exists := server.state.Messages[messageKey(555, base.server.state.NextMessageID)]; exists {
			t.Fatal("unconfirmed message was committed")
		}
	})

	t.Run("unrebuildable", func(t *testing.T) {
		root := t.TempDir()
		base := newBaseline(t, root)
		documentID := base.server.state.NextDocumentID
		extra := []byte("bytes lost")
		commit := newCommit(base.server, base.channel, documentID, base.server.state.NextMessageID, 4242, 1, extra)
		writeIntent(t, base.server, commit) // no staged blob, no parts

		server := reopen(t, root)
		report := server.RecoveryReport()
		if report.ReplayedCommits != 0 || len(report.Quarantined) != 1 {
			t.Fatalf("recovery report = %#v", report)
		}
		if _, exists := server.state.Documents[documentKey(documentID)]; exists {
			t.Fatal("unconfirmed document was committed")
		}
	})
}

// A lingering intent for an already committed message is cleaned up, never
// republished with a new ID.
func TestRecoverCommittedIntentIsIdempotent(t *testing.T) {
	root := t.TempDir()
	base := newBaseline(t, root)
	record := base.server.state.Documents[documentKey(10000)]
	messageID := 1
	commit := pendingCommit{
		Version: stateVersion, ChannelID: base.channel, MessageID: messageID,
		UploadFileID: 1, UploadParts: 1, Document: record, CreatedAt: record.CreatedAt,
	}
	writeIntent(t, base.server, commit)

	server := reopen(t, root)
	report := server.RecoveryReport()
	if report.ReplayedCommits != 0 || len(report.Quarantined) != 0 {
		t.Fatalf("recovery report = %#v", report)
	}
	if len(server.state.Messages) != 1 || len(server.state.Documents) != 1 {
		t.Fatalf("confirmed message republished: messages=%d documents=%d", len(server.state.Messages), len(server.state.Documents))
	}
	runner, _ := NewRunner(server)
	downloadPart(t, telegramstore.NewGotdStorage(runner, nil), base.channel, int64(messageID), base.payload)
}

// Index entries pointing at missing or corrupt blobs are dropped and recorded
// as observable dangling references; the root still opens and stays ready.
func TestRecoverRepairsDanglingReferences(t *testing.T) {
	t.Run("missing blob", func(t *testing.T) {
		root := t.TempDir()
		base := newBaseline(t, root)
		if err := os.Remove(filepath.Join(base.server.documentsDir, "10000.bin")); err != nil {
			t.Fatal(err)
		}

		server := reopen(t, root)
		report := server.RecoveryReport()
		if len(report.DanglingReferences) != 1 || report.DanglingReferences[0].Reason != reasonMissingDocument {
			t.Fatalf("recovery report = %#v", report)
		}
		if len(server.state.Messages) != 0 || len(server.state.Documents) != 0 {
			t.Fatalf("dangling index retained: %#v", report)
		}
		if entries := dirEntries(t, server.quarantineDir); len(entries) == 0 {
			t.Fatal("dangling manifest not written")
		}
		if err := server.Ping(context.Background()); err != nil {
			t.Fatalf("Ping() error = %v", err)
		}
	})

	t.Run("corrupt blob", func(t *testing.T) {
		root := t.TempDir()
		base := newBaseline(t, root)
		path := filepath.Join(base.server.documentsDir, "10000.bin")
		corrupt, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		corrupt[0] ^= 0xFF
		writeBlob(t, path, corrupt)

		server := reopen(t, root)
		report := server.RecoveryReport()
		if len(report.DanglingReferences) != 1 || report.DanglingReferences[0].Reason != reasonCorruptDocument {
			t.Fatalf("recovery report = %#v", report)
		}
		var foundCorruptBlob bool
		for _, entry := range report.Quarantined {
			if entry.Reason == reasonCorruptDocument {
				foundCorruptBlob = true
			}
		}
		if !foundCorruptBlob {
			t.Fatalf("corrupt blob not quarantined: %#v", report.Quarantined)
		}
		if entries := dirEntries(t, server.documentsDir); len(entries) != 0 {
			t.Fatalf("corrupt blob retained: %v", entries)
		}
	})
}

// State-first deletes can leave committed-unreferenced blobs after a crash;
// unindexed files of any shape are quarantined, never deleted silently.
func TestRecoverQuarantinesOrphanAndStrangeFiles(t *testing.T) {
	root := t.TempDir()
	base := newBaseline(t, root)
	writeBlob(t, filepath.Join(base.server.documentsDir, "10001.bin"), []byte("leftover after half delete"))
	writeBlob(t, filepath.Join(base.server.documentsDir, ".document-old.tmp"), []byte("old assembly temp"))
	writeBlob(t, filepath.Join(base.server.pendingDir, "10002.blob"), []byte("staged without intent"))
	writeBlob(t, filepath.Join(base.server.pendingDir, ".blob-123.tmp"), []byte("interrupted staging"))

	server := reopen(t, root)
	reasons := map[string]int{}
	for _, entry := range server.RecoveryReport().Quarantined {
		reasons[entry.Reason]++
	}
	if reasons[reasonOrphanDocument] != 1 || reasons[reasonUnknownDocument] != 1 ||
		reasons[reasonStagedWithoutCommit] != 1 || reasons[reasonInterruptedStaging] != 1 {
		t.Fatalf("quarantine reasons = %#v", reasons)
	}
	// The confirmed message and its blob survive.
	if len(server.state.Messages) != 1 {
		t.Fatalf("confirmed messages lost: %d", len(server.state.Messages))
	}
	runner, _ := NewRunner(server)
	downloadPart(t, telegramstore.NewGotdStorage(runner, nil), base.channel, base.stored.MessageID, base.payload)
}

// Messages surviving in a deleted channel are dropped; documents that only
// those messages referenced are detached and their blobs quarantined.
func TestRecoverDropsMessagesInMissingChannel(t *testing.T) {
	root := t.TempDir()
	base := newBaseline(t, root)

	// Commit a second blob plus a message in a channel that never existed.
	orphanPayload := []byte("belongs to a vanished channel")
	orphanPath := filepath.Join(base.server.documentsDir, "10001.bin")
	writeBlob(t, orphanPath, orphanPayload)
	orphanRecord := documentRecord{
		ID: 10001, AccessHash: 10001 + 1_000_000,
		FileReference: []byte(sha256Hex(orphanPayload)), MimeType: "application/octet-stream",
		Size: int64(len(orphanPayload)), DCID: 1, FileName: "ghost.bin", CreatedAt: 1700000001,
	}
	base.server.state.Documents[documentKey(10001)] = orphanRecord
	base.server.state.Messages[messageKey(999, 1)] = messageRecord{ChannelID: 999, ID: 1, DocumentID: 10001, CreatedAt: 1700000001}
	base.server.state.NextDocumentID = 10002
	if err := saveState(base.server.statePath, base.server.state); err != nil {
		t.Fatal(err)
	}

	server := reopen(t, root)
	report := server.RecoveryReport()
	var foundDangling, foundOrphan bool
	for _, dangling := range report.DanglingReferences {
		if dangling.Reason == reasonDanglingMessage {
			foundDangling = true
		}
	}
	for _, quarantined := range report.Quarantined {
		if quarantined.Reason == reasonOrphanDocument {
			foundOrphan = true
		}
	}
	if !foundDangling || !foundOrphan {
		t.Fatalf("recovery report = %#v", report)
	}
	if _, exists := server.state.Messages[messageKey(999, 1)]; exists {
		t.Fatal("message in missing channel retained")
	}
	if _, exists := server.state.Documents[documentKey(10001)]; exists {
		t.Fatal("detached document record retained")
	}
	if _, exists := server.state.Messages[messageKey(base.channel, 1)]; !exists {
		t.Fatal("confirmed message lost while repairing another channel")
	}
}

// In-flight upload chunks from killed sessions are always discarded.
func TestRecoverDiscardsIncompleteUploads(t *testing.T) {
	root := t.TempDir()
	base := newBaseline(t, root)
	writeParts(t, base.server, 31337, []byte("chunkchunkchunk"), 5)

	server := reopen(t, root)
	if report := server.RecoveryReport(); report.DiscardedUploads != 1 {
		t.Fatalf("recovery report = %#v", report)
	}
	if entries := dirEntries(t, server.uploadsDir); len(entries) != 0 {
		t.Fatalf("uploads remain: %v", entries)
	}
}

// Interrupted state writes and upgrades leave recognizable artifacts.
func TestRecoverStateArtifacts(t *testing.T) {
	t.Run("stale state temp removed", func(t *testing.T) {
		root := t.TempDir()
		base := newBaseline(t, root)
		writeBlob(t, filepath.Join(base.server.root, ".state-xyz.json"), []byte("{broken"))

		server := reopen(t, root)
		if entries := dirEntries(t, server.root); contains(entries, ".state-xyz.json") {
			t.Fatalf("stale state temp retained: %v", entries)
		}
	})

	t.Run("corrupt index restored from backup", func(t *testing.T) {
		root := t.TempDir()
		base := newBaseline(t, root)
		backup, err := os.ReadFile(base.server.statePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(stateBackupPath(base.server.statePath), backup, 0o600); err != nil {
			t.Fatal(err)
		}
		writeBlob(t, base.server.statePath, []byte("{not json"))

		server := reopen(t, root)
		if report := server.RecoveryReport(); !report.RestoredFromBackup {
			t.Fatalf("recovery report = %#v", report)
		}
		if len(server.state.Messages) != 1 {
			t.Fatalf("restored index lost data: %#v", server.RecoveryReport())
		}
		if fileExists(stateBackupPath(server.statePath)) {
			t.Fatal("backup retained after successful recovery")
		}
	})

	t.Run("future version rejected", func(t *testing.T) {
		root := t.TempDir()
		base := newBaseline(t, root)
		future := base.server.state
		future.Version = stateVersion + 10
		payload, _ := json.Marshal(future)
		writeBlob(t, base.server.statePath, payload)

		if _, err := Open(root); err == nil {
			t.Fatal("Open() accepted unsupported state version")
		}
	})
}

// Ping refuses to report a damaged root as ready.
func TestPingRejectsBrokenRoot(t *testing.T) {
	root := t.TempDir()
	base := newBaseline(t, root)
	if err := base.server.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if err := os.Remove(base.server.statePath); err != nil {
		t.Fatal(err)
	}
	if err := base.server.Ping(context.Background()); err == nil {
		t.Fatal("Ping() passed without a state index")
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// The orphan cleanup traverses storage through GotdStorage.ListDocumentMessages
// (messages.getHistory under the hood). The local backend must paginate local
// messages the same way Telegram would, so the sweep can see every referenced
// document and refrain from deleting messages the database still uses.
func TestLocalMessageHistoryPaginatesLikeTelegram(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	base := newBaseline(t, root)

	extraIDs := make([]int64, 0, 4)
	for range 4 {
		payload := []byte(fmt.Sprintf("document-%d", len(extraIDs)))
		stored, err := base.storage.Upload(ctx, telegramstore.UploadRequest{
			UserID: 1001, ChannelID: base.channel, Name: "doc.bin",
			Reader: bytes.NewReader(payload), Size: int64(len(payload)), Threads: 1,
		})
		if err != nil {
			t.Fatalf("Upload() error = %v", err)
		}
		extraIDs = append(extraIDs, stored.MessageID)
	}

	client := base.server.Client()
	peer := &tg.InputPeerChannel{ChannelID: base.channel, AccessHash: base.channel + 1_000_000}

	// Raw history semantics: descending IDs, strictly older than OffsetID.
	first, err := client.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peer, Limit: 2})
	if err != nil {
		t.Fatalf("MessagesGetHistory() error = %v", err)
	}
	modified, ok := first.AsModified()
	if !ok {
		t.Fatalf("history type %T", first)
	}
	if ids := messageClassIDs(modified.GetMessages()); len(ids) != 2 || ids[0] != 5 || ids[1] != 4 {
		t.Fatalf("first page ids = %v", ids)
	}
	second, err := client.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peer, OffsetID: 4, Limit: 2})
	if err != nil {
		t.Fatalf("MessagesGetHistory(page 2) error = %v", err)
	}
	modified2, _ := second.AsModified()
	if ids := messageClassIDs(modified2.GetMessages()); len(ids) != 2 || ids[0] != 3 || ids[1] != 2 {
		t.Fatalf("second page ids = %v", ids)
	}

	// Storage-level lister contract used by the cleanup worker.
	lister, ok := base.storage.(telegramstore.DocumentMessageLister)
	if !ok {
		t.Fatal("local GotdStorage does not implement DocumentMessageLister")
	}
	seen := map[int64]struct{}{}
	beforeID := int64(0)
	pages := 0
	for {
		page, err := lister.ListDocumentMessages(ctx, telegramstore.ListDocumentMessagesRequest{
			UserID: 1001, ChannelID: base.channel, BeforeID: beforeID, Limit: 2,
		})
		if err != nil {
			t.Fatalf("ListDocumentMessages() error = %v", err)
		}
		pages++
		for _, message := range page.Messages {
			if beforeID != 0 && message.ID >= beforeID {
				t.Fatalf("page returned id %d not older than cursor %d", message.ID, beforeID)
			}
			seen[message.ID] = struct{}{}
		}
		if page.Exhausted {
			break
		}
		if page.BeforeID <= 0 || page.BeforeID == beforeID {
			t.Fatalf("pagination did not advance: %d", page.BeforeID)
		}
		beforeID = page.BeforeID
	}
	if pages != 3 || len(seen) != 5 {
		t.Fatalf("traversal pages=%d seen=%d", pages, len(seen))
	}
	for _, id := range append(extraIDs, base.stored.MessageID) {
		if _, ok := seen[id]; !ok {
			t.Fatalf("referenced message %d not enumerated; cleanup could have deleted it", id)
		}
	}
}

func messageClassIDs(messages []tg.MessageClass) []int {
	ids := make([]int, 0, len(messages))
	for _, message := range messages {
		plain, ok := message.(*tg.Message)
		if !ok {
			continue
		}
		ids = append(ids, plain.ID)
	}
	return ids
}
