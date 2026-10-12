package localtelegram

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Recovery reasons recorded for quarantined content.
const (
	reasonOrphanDocument      = "orphan-document"
	reasonCorruptDocument     = "corrupt-document"
	reasonUnknownDocument     = "unknown-document-file"
	reasonStagedWithoutCommit = "staged-without-commit"
	reasonInterruptedStaging  = "interrupted-staging"
	reasonUnconfirmedCommit   = "unconfirmed-commit"
	reasonMalformedIntent     = "malformed-intent"
	reasonDanglingMessage     = "dangling-message"
	reasonMissingDocument     = "missing-document"
	reasonUnknownPendingFile  = "unknown-pending-file"
)

// QuarantineEntry describes one item moved into the quarantine directory.
type QuarantineEntry struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
	Source string `json:"source"`
}

// DanglingReference describes index entries that pointed at unusable content
// and were dropped to converge the root to a consistent committed view.
type DanglingReference struct {
	Reason     string   `json:"reason"`
	DocumentID int64    `json:"documentId,omitempty"`
	Messages   []string `json:"messages,omitempty"`
	Detail     string   `json:"detail,omitempty"`
}

// RecoveryReport summarizes what startup reconciliation did. It is available
// to operators through Server.RecoveryReport and application logs.
type RecoveryReport struct {
	ReplayedCommits    int                 `json:"replayedCommits"`
	DiscardedUploads   int                 `json:"discardedUploads"`
	Quarantined        []QuarantineEntry   `json:"quarantined"`
	DanglingReferences []DanglingReference `json:"danglingReferences"`
	UpgradedFrom       int                 `json:"upgradedFrom,omitempty"`
	RestoredFromBackup bool                `json:"restoredFromBackup"`
}

func (s *Server) recover() error {
	report := RecoveryReport{}

	if err := removeStateTempFiles(s.root); err != nil {
		return err
	}

	state, fresh, err := s.loadCommittedState(&report)
	if err != nil {
		return err
	}

	originalVersion := state.Version
	backupAlreadyExists := fileExists(stateBackupPath(s.statePath))
	if !fresh && state.Version < stateVersion && !backupAlreadyExists {
		// Back up the pre-upgrade bytes before rewriting the committed index,
		// so an interrupted upgrade can redo from the old version.
		if err := copyFile(s.statePath, stateBackupPath(s.statePath), 0o600); err != nil {
			return fmt.Errorf("back up local Telegram state before upgrade: %w", err)
		}
		if err := syncDir(s.root); err != nil {
			return fmt.Errorf("sync local Telegram state backup: %w", err)
		}
		backupAlreadyExists = true
	}

	intentFiles, stagedBlobs, stagingTemps, unknownPending, err := scanPendingDir(s.pendingDir)
	if err != nil {
		return err
	}
	stagedByID := make(map[int64]string)
	for _, blob := range stagedBlobs {
		if blob.parseOK {
			stagedByID[blob.documentID] = blob.name
		}
	}

	// Replay durable publish intents first; their staged blob and upload parts
	// let interrupted publishes complete instead of being republished.
	consumedStaged := make(map[string]bool)
	for _, file := range intentFiles {
		outcome, err := s.replayIntent(file, stagedByID, state)
		if err != nil {
			return err
		}
		if outcome.replayed {
			report.ReplayedCommits++
		}
		if outcome.quarantined != nil {
			report.Quarantined = append(report.Quarantined, *outcome.quarantined)
		}
		if outcome.blobQuarantined != nil {
			report.Quarantined = append(report.Quarantined, *outcome.blobQuarantined)
		}
		if outcome.staged != "" {
			consumedStaged[outcome.staged] = true
		}
		if outcome.discardedUpload != "" {
			if err := os.RemoveAll(filepath.Join(s.uploadsDir, outcome.discardedUpload)); err != nil {
				return fmt.Errorf("discard local Telegram upload %q: %w", outcome.discardedUpload, err)
			}
			report.DiscardedUploads++
		}
	}

	// Repair index entries pointing at missing or corrupt blobs, and messages
	// whose owning channel vanished. Bytes cannot be fabricated, so the
	// consistent resolution is recorded as an observable manifest.
	dangling, quarantinedByRepair, err := s.repairDanglingReferences(state)
	if err != nil {
		return err
	}
	report.DanglingReferences = append(report.DanglingReferences, dangling...)
	report.Quarantined = append(report.Quarantined, quarantinedByRepair...)

	// Committed blobs without an index entry are leftovers from interrupted
	// publishes or state-first deletes; ownership cannot be proven, so they
	// are quarantined rather than deleted.
	orphaned, err := s.quarantineOrphanDocuments(state)
	if err != nil {
		return err
	}
	report.Quarantined = append(report.Quarantined, orphaned...)

	// Everything still in pending/ now is unowned: assembly temps, staged
	// blobs whose intent never landed, and unexpected files.
	leftover := make([]pendingEntry, 0, len(stagingTemps)+len(stagedBlobs)+len(unknownPending))
	leftover = append(leftover, stagingTemps...)
	for _, blob := range stagedBlobs {
		if !consumedStaged[blob.name] {
			leftover = append(leftover, blob)
		}
	}
	leftover = append(leftover, unknownPending...)
	slices.SortFunc(leftover, func(a, b pendingEntry) int { return strings.Compare(a.name, b.name) })
	for _, entry := range leftover {
		reason := reasonUnknownPendingFile
		switch {
		case strings.HasPrefix(entry.name, stagedTempPrefix):
			reason = reasonInterruptedStaging
		case strings.HasSuffix(entry.name, stagedBlobSuffix):
			reason = reasonStagedWithoutCommit
		}
		quarantined, err := s.quarantinePath(filepath.Join(s.pendingDir, entry.name), reason, "")
		if err != nil {
			return err
		}
		report.Quarantined = append(report.Quarantined, quarantined)
	}

	discarded, err := discardIncompleteUploads(s.uploadsDir)
	if err != nil {
		return err
	}
	report.DiscardedUploads += discarded

	state.Version = stateVersion
	pinCounters(&state)
	if originalVersion < stateVersion {
		report.UpgradedFrom = originalVersion
	}

	if err := saveState(s.statePath, state); err != nil {
		return err
	}
	if backupAlreadyExists {
		// The upgraded/restored index is now durably committed; the backup
		// existed only to survive an interrupted upgrade.
		if err := os.Remove(stateBackupPath(s.statePath)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove local Telegram state backup: %w", err)
		}
		if err := syncDir(s.root); err != nil {
			return err
		}
	}

	s.state = state
	s.report = report
	return nil
}

func (s *Server) loadCommittedState(report *RecoveryReport) (persistedState, bool, error) {
	data, err := os.ReadFile(s.statePath)
	if errors.Is(err, os.ErrNotExist) {
		if state, ok := s.restoreBackupState(report); ok {
			return state, false, nil
		}
		return newState(), true, nil
	}
	if err != nil {
		return persistedState{}, false, fmt.Errorf("read local Telegram state: %w", err)
	}
	state, decodeErr := decodeState(data)
	if decodeErr != nil {
		// Corrupt committed index: an upgrade backup is the last known good view.
		if restored, ok := s.restoreBackupState(report); ok {
			return restored, false, nil
		}
		return persistedState{}, false, fmt.Errorf("decode local Telegram state (no usable backup): %w", decodeErr)
	}
	if err := validateVersion(state); err != nil {
		return persistedState{}, false, err
	}
	return state, false, nil
}

func (s *Server) restoreBackupState(report *RecoveryReport) (persistedState, bool) {
	backupData, err := os.ReadFile(stateBackupPath(s.statePath))
	if err != nil {
		return persistedState{}, false
	}
	state, err := decodeState(backupData)
	if err != nil {
		return persistedState{}, false
	}
	if err := validateVersion(state); err != nil {
		return persistedState{}, false
	}
	report.RestoredFromBackup = true
	return state, true
}

// removeStateTempFiles deletes interrupted atomic state writes; state.json
// itself is only ever replaced atomically, so these temps are never committed.
func removeStateTempFiles(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("read local Telegram root: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".state-") && strings.HasSuffix(name, ".json") {
			if err := os.Remove(filepath.Join(root, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove interrupted local Telegram state write %q: %w", name, err)
			}
		}
	}
	return nil
}

type pendingEntry struct {
	name       string
	documentID int64
	parseOK    bool
}

func scanPendingDir(dir string) (intents, stagedBlobs, stagingTemps, unknown []pendingEntry, err error) {
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		return nil, nil, nil, nil, fmt.Errorf("read local Telegram pending directory: %w", readErr)
	}
	for _, entry := range entries {
		item := pendingEntry{name: entry.Name()}
		switch {
		case entry.IsDir():
			unknown = append(unknown, item)
		case strings.HasSuffix(entry.Name(), ".json"):
			item.documentID, item.parseOK = parseStagingID(strings.TrimSuffix(entry.Name(), ".json"))
			intents = append(intents, item)
		case strings.HasSuffix(entry.Name(), stagedBlobSuffix):
			item.documentID, item.parseOK = parseStagingID(strings.TrimSuffix(entry.Name(), stagedBlobSuffix))
			stagedBlobs = append(stagedBlobs, item)
		case strings.HasPrefix(entry.Name(), stagedTempPrefix):
			stagingTemps = append(stagingTemps, item)
		default:
			unknown = append(unknown, item)
		}
	}
	slices.SortFunc(intents, func(a, b pendingEntry) int { return cmpInt64(a.documentID, b.documentID) })
	return intents, stagedBlobs, stagingTemps, unknown, nil
}

func parseStagingID(value string) (int64, bool) {
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

type intentOutcome struct {
	replayed        bool
	staged          string
	discardedUpload string
	quarantined     *QuarantineEntry
	blobQuarantined *QuarantineEntry
}

// replayIntent finishes one durable publish intent, or preserves it for
// inspection when the publish cannot be proven complete.
func (s *Server) replayIntent(intentFile pendingEntry, stagedByID map[int64]string, state persistedState) (intentOutcome, error) {
	outcome := intentOutcome{}
	intentPath := filepath.Join(s.pendingDir, intentFile.name)

	raw, err := os.ReadFile(intentPath)
	if err != nil {
		return outcome, fmt.Errorf("read local Telegram publish intent %q: %w", intentFile.name, err)
	}
	var commit pendingCommit
	if err := json.Unmarshal(raw, &commit); err != nil {
		return s.abandonIntent(intentPath, intentFile, stagedByID, reasonMalformedIntent,
			fmt.Sprintf("decode intent: %v", err), 0, outcome)
	}
	if commit.Version != stateVersion || commit.Document.ID <= 0 || commit.MessageID <= 0 ||
		commit.ChannelID == 0 || !intentFile.parseOK || commit.Document.ID != intentFile.documentID {
		return s.abandonIntent(intentPath, intentFile, stagedByID, reasonMalformedIntent,
			"intent fields are invalid", commit.UploadFileID, outcome)
	}

	msgKey := messageKey(commit.ChannelID, commit.MessageID)
	docKey := documentKey(commit.Document.ID)
	if existingMessage, exists := state.Messages[msgKey]; exists {
		if existingMessage.DocumentID != commit.Document.ID {
			return outcome, fmt.Errorf("local Telegram message %s is already committed to document %d; refusing to overwrite",
				msgKey, existingMessage.DocumentID)
		}
		if existingDocument, ok := state.Documents[docKey]; ok && !sameDocument(existingDocument, commit.Document) {
			return outcome, fmt.Errorf("local Telegram document %d is already committed with different content", commit.Document.ID)
		}
		// Already committed: only artifact cleanup remains; reopening never
		// republishes a confirmed message.
		if name, ok := stagedByID[commit.Document.ID]; ok {
			outcome.staged = name
		}
		if err := s.removePublishArtifacts(commit); err != nil {
			return outcome, err
		}
		return outcome, nil
	}
	if existingDocument, ok := state.Documents[docKey]; ok && !sameDocument(existingDocument, commit.Document) {
		return outcome, fmt.Errorf("local Telegram document %d is already committed with different content", commit.Document.ID)
	}
	if _, channelExists := state.Channels[channelKey(commit.ChannelID)]; !channelExists {
		// The channel commit never landed; this publish was never confirmed.
		return s.abandonIntent(intentPath, intentFile, stagedByID, reasonUnconfirmedCommit,
			fmt.Sprintf("channel %d does not exist", commit.ChannelID), commit.UploadFileID, outcome)
	}

	installedPath := filepath.Join(s.documentsDir, fmt.Sprintf("%d.bin", commit.Document.ID))
	stagedName, hasStaged := stagedByID[commit.Document.ID]
	stagedPath := filepath.Join(s.pendingDir, stagingBlobName(commit.Document.ID))
	switch {
	case blobUsable(commit.Document, installedPath):
		// Publish reached the installed blob before interruption. A staged
		// copy left beside it belongs to the same commit and is cleaned up.
		if hasStaged {
			outcome.staged = stagedName
			if err := os.Remove(stagedPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return outcome, fmt.Errorf("remove redundant local Telegram staged blob: %w", err)
			}
			if err := syncDir(s.pendingDir); err != nil {
				return outcome, err
			}
		}
	case hasStaged && blobUsable(commit.Document, stagedPath):
		if err := replaceFile(stagedPath, installedPath); err != nil {
			return outcome, fmt.Errorf("install local Telegram document %d: %w", commit.Document.ID, err)
		}
		outcome.staged = stagedName
	default:
		// Last chance: rebuild the staged blob from the original upload parts.
		if err := s.assembleFromParts(commit, stagedPath); err != nil {
			return s.abandonIntent(intentPath, intentFile, stagedByID, reasonUnconfirmedCommit,
				fmt.Sprintf("rebuild document %d: %v", commit.Document.ID, err), commit.UploadFileID, outcome)
		}
		if err := replaceFile(stagedPath, installedPath); err != nil {
			return outcome, fmt.Errorf("install rebuilt local Telegram document %d: %w", commit.Document.ID, err)
		}
		outcome.staged = stagingBlobName(commit.Document.ID)
	}

	state.Documents[docKey] = commit.Document
	state.Messages[msgKey] = messageRecord{
		ChannelID:  commit.ChannelID,
		ID:         commit.MessageID,
		DocumentID: commit.Document.ID,
		CreatedAt:  commit.CreatedAt,
	}
	if err := s.removePublishArtifacts(commit); err != nil {
		return outcome, err
	}
	outcome.replayed = true
	return outcome, nil
}

// abandonIntent quarantines an intent that cannot be completed and discards
// the upload session that belonged to it.
func (s *Server) abandonIntent(intentPath string, intentFile pendingEntry, stagedByID map[int64]string,
	reason, detail string, uploadFileID int64, outcome intentOutcome) (intentOutcome, error) {
	entry, err := s.quarantinePath(intentPath, reason, detail)
	if err != nil {
		return outcome, err
	}
	outcome.quarantined = &entry
	if intentFile.parseOK {
		if name, ok := stagedByID[intentFile.documentID]; ok {
			blobEntry, err := s.quarantinePath(filepath.Join(s.pendingDir, name), reasonStagedWithoutCommit,
				"staged blob belonging to "+reason+" intent "+intentFile.name)
			if err != nil {
				return outcome, err
			}
			outcome.blobQuarantined = &blobEntry
			outcome.staged = name
		}
	}
	if uploadFileID != 0 {
		uploadName := fmt.Sprintf("%d", uploadFileID)
		if fileExists(filepath.Join(s.uploadsDir, uploadName)) {
			outcome.discardedUpload = uploadName
		}
	}
	return outcome, nil
}

func blobUsable(record documentRecord, path string) bool {
	ok, err := documentMatchesBlob(record, path)
	return err == nil && ok
}

func stagingBlobName(documentID int64) string {
	return fmt.Sprintf("%d%s", documentID, stagedBlobSuffix)
}

// removePublishArtifacts deletes the intent file, staged blob and the upload
// parts directory of a finished (or already committed) publish.
func (s *Server) removePublishArtifacts(commit pendingCommit) error {
	if err := os.Remove(filepath.Join(s.pendingDir, fmt.Sprintf("%d.json", commit.Document.ID))); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove local Telegram publish intent: %w", err)
	}
	if err := os.Remove(filepath.Join(s.pendingDir, stagingBlobName(commit.Document.ID))); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove local Telegram staged blob: %w", err)
	}
	if err := syncDir(s.pendingDir); err != nil {
		return err
	}
	if commit.UploadFileID != 0 {
		uploadDir := filepath.Join(s.uploadsDir, fmt.Sprintf("%d", commit.UploadFileID))
		if fileExists(uploadDir) {
			if err := os.RemoveAll(uploadDir); err != nil {
				return fmt.Errorf("remove local Telegram upload parts: %w", err)
			}
		}
	}
	return nil
}

// repairDanglingReferences drops messages/documents whose blob is missing or
// corrupt and messages whose channel is absent, recording manifests.
func (s *Server) repairDanglingReferences(state persistedState) ([]DanglingReference, []QuarantineEntry, error) {
	var reports []DanglingReference
	var quarantined []QuarantineEntry

	documentMessages := make(map[int64][]string)
	for key, message := range state.Messages {
		documentMessages[message.DocumentID] = append(documentMessages[message.DocumentID], key)
	}

	documentIDs := make([]int64, 0, len(state.Documents))
	for _, document := range state.Documents {
		documentIDs = append(documentIDs, document.ID)
	}
	slices.Sort(documentIDs)
	for _, id := range documentIDs {
		key := documentKey(id)
		document := state.Documents[key]
		path := filepath.Join(s.documentsDir, fmt.Sprintf("%d.bin", document.ID))
		ok, verifyErr := documentMatchesBlob(document, path)
		if ok {
			continue
		}
		if errors.Is(verifyErr, os.ErrNotExist) {
			report := DanglingReference{
				Reason: reasonMissingDocument, DocumentID: document.ID,
				Messages: sortedStrings(documentMessages[document.ID]),
				Detail:   fmt.Sprintf("document %d is indexed but its blob is missing", document.ID),
			}
			if err := s.writeDanglingManifest(report); err != nil {
				return nil, nil, err
			}
			reports = append(reports, report)
		} else {
			detail := fmt.Sprintf("document %d failed size/hash verification: %v", document.ID, verifyErr)
			entry, err := s.quarantinePath(path, reasonCorruptDocument, detail)
			if err != nil {
				return nil, nil, err
			}
			quarantined = append(quarantined, entry)
			report := DanglingReference{
				Reason: reasonCorruptDocument, DocumentID: document.ID,
				Messages: sortedStrings(documentMessages[document.ID]), Detail: detail,
			}
			if err := s.writeDanglingManifest(report); err != nil {
				return nil, nil, err
			}
			reports = append(reports, report)
		}
		for _, messageKey := range documentMessages[document.ID] {
			delete(state.Messages, messageKey)
		}
		delete(state.Documents, key)
	}

	messageKeys := make([]string, 0, len(state.Messages))
	for key := range state.Messages {
		messageKeys = append(messageKeys, key)
	}
	slices.Sort(messageKeys)
	for _, key := range messageKeys {
		message := state.Messages[key]
		var detail string
		if _, channelExists := state.Channels[channelKey(message.ChannelID)]; !channelExists {
			detail = fmt.Sprintf("channel %d does not exist", message.ChannelID)
		} else if _, documentExists := state.Documents[documentKey(message.DocumentID)]; !documentExists {
			detail = fmt.Sprintf("document %d does not exist", message.DocumentID)
		} else {
			continue
		}
		report := DanglingReference{
			Reason: reasonDanglingMessage, DocumentID: message.DocumentID, Messages: []string{key},
			Detail: detail,
		}
		if err := s.writeDanglingManifest(report); err != nil {
			return nil, nil, err
		}
		reports = append(reports, report)
		delete(state.Messages, key)
	}

	// Dropping messages can orphan document records the messages referenced.
	// Detach them here; their on-disk blobs are quarantined by the orphan
	// sweep that follows, so nothing indexed-but-unreachable accumulates.
	referenced := make(map[int64]struct{}, len(state.Messages))
	for _, message := range state.Messages {
		referenced[message.DocumentID] = struct{}{}
	}
	for key, document := range state.Documents {
		if _, ok := referenced[document.ID]; ok {
			continue
		}
		delete(state.Documents, key)
	}

	return reports, quarantined, nil
}

func sortedStrings(values []string) []string {
	slices.Sort(values)
	return values
}

func (s *Server) quarantineOrphanDocuments(state persistedState) ([]QuarantineEntry, error) {
	entries, err := os.ReadDir(s.documentsDir)
	if err != nil {
		return nil, fmt.Errorf("read local Telegram documents directory: %w", err)
	}
	var quarantined []QuarantineEntry
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		id, parseErr := parseDocumentBlobName(name)
		if parseErr != nil {
			quarantinedEntry, err := s.quarantinePath(filepath.Join(s.documentsDir, name), reasonUnknownDocument,
				"unexpected file in documents directory")
			if err != nil {
				return nil, err
			}
			quarantined = append(quarantined, quarantinedEntry)
			continue
		}
		if _, indexed := state.Documents[documentKey(id)]; indexed {
			continue
		}
		quarantinedEntry, err := s.quarantinePath(filepath.Join(s.documentsDir, name), reasonOrphanDocument,
			fmt.Sprintf("document %d has no index entry or message reference", id))
		if err != nil {
			return nil, err
		}
		quarantined = append(quarantined, quarantinedEntry)
	}
	return quarantined, nil
}

func parseDocumentBlobName(name string) (int64, error) {
	if !strings.HasSuffix(name, ".bin") {
		return 0, fmt.Errorf("unexpected name %q", name)
	}
	id, err := strconv.ParseInt(strings.TrimSuffix(name, ".bin"), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("unexpected name %q", name)
	}
	return id, nil
}

func discardIncompleteUploads(uploadsDir string) (int, error) {
	entries, err := os.ReadDir(uploadsDir)
	if err != nil {
		return 0, fmt.Errorf("read local Telegram uploads: %w", err)
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if err := os.RemoveAll(filepath.Join(uploadsDir, entry.Name())); err != nil {
			return 0, fmt.Errorf("remove incomplete local Telegram upload %q: %w", entry.Name(), err)
		}
		count++
	}
	return count, nil
}

// quarantinePath moves source into the quarantine directory using a
// reason-prefixed name and writes a JSON manifest describing it. When the
// source no longer exists, only the manifest is written.
func (s *Server) quarantinePath(source, reason, detail string) (QuarantineEntry, error) {
	if err := os.MkdirAll(s.quarantineDir, 0o700); err != nil {
		return QuarantineEntry{}, fmt.Errorf("create local Telegram quarantine directory: %w", err)
	}
	base := filepath.Base(source)
	timestamp := time.Now().UnixNano()
	entry := QuarantineEntry{Reason: reason, Source: source}

	destination := filepath.Join(s.quarantineDir, fmt.Sprintf("%s-%s-%d", reason, base, timestamp))
	renameErr := os.Rename(source, destination)
	if renameErr != nil && !errors.Is(renameErr, os.ErrNotExist) {
		return entry, fmt.Errorf("quarantine %q: %w", base, renameErr)
	}
	if renameErr == nil {
		entry.Name = filepath.Base(destination)
	}

	manifest := quarantineManifest{Reason: reason, DetectedAt: time.Now().UTC().Format(time.RFC3339Nano), Source: source, Detail: detail}
	manifestName := filepath.Base(destination) + ".json"
	if err := writeAtomicFile(filepath.Join(s.quarantineDir, manifestName), mustMarshalManifest(manifest), 0o600); err != nil {
		return entry, fmt.Errorf("write quarantine manifest for %q: %w", base, err)
	}
	if renameErr != nil {
		entry.Name = manifestName
	}
	return entry, nil
}

func (s *Server) writeDanglingManifest(report DanglingReference) error {
	if err := os.MkdirAll(s.quarantineDir, 0o700); err != nil {
		return fmt.Errorf("create local Telegram quarantine directory: %w", err)
	}
	payload, err := json.MarshalIndent(struct {
		Reason     string   `json:"reason"`
		DetectedAt string   `json:"detectedAt"`
		Source     string   `json:"source,omitempty"`
		Detail     string   `json:"detail,omitempty"`
		DocumentID int64    `json:"documentId,omitempty"`
		Messages   []string `json:"messages,omitempty"`
	}{
		Reason:     report.Reason,
		DetectedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Detail:     report.Detail,
		DocumentID: report.DocumentID,
		Messages:   report.Messages,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode dangling reference manifest: %w", err)
	}
	name := fmt.Sprintf("%s-%d-%d.json", report.Reason, report.DocumentID, time.Now().UnixNano())
	if err := writeAtomicFile(filepath.Join(s.quarantineDir, name), append(payload, '\n'), 0o600); err != nil {
		return fmt.Errorf("write dangling reference manifest: %w", err)
	}
	return nil
}

func mustMarshalManifest(manifest quarantineManifest) []byte {
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return []byte(`{"reason":"` + manifest.Reason + `"}` + "\n")
	}
	return append(payload, '\n')
}
