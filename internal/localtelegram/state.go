package localtelegram

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

const (
	stateVersion = 1

	stateFileName     = "state.json"
	stateBackupSuffix = ".pre-upgrade"
	uploadsDirName    = "uploads"
	documentsDirName  = "documents"
	pendingDirName    = "pending"
	quarantineDirName = "quarantine"

	// Staged document blobs awaiting their index commit live in the pending
	// directory as <documentId>.blob; their atomic write temp uses this prefix.
	stagedBlobSuffix = ".blob"
	stagedTempPrefix = ".blob-"
)

type persistedState struct {
	Version        int                       `json:"version"`
	NextChannelID  int64                     `json:"nextChannelId"`
	NextDocumentID int64                     `json:"nextDocumentId"`
	NextMessageID  int                       `json:"nextMessageId"`
	Channels       map[string]channelRecord  `json:"channels"`
	Documents      map[string]documentRecord `json:"documents"`
	Messages       map[string]messageRecord  `json:"messages"`
}

type channelRecord struct {
	ID         int64  `json:"id"`
	AccessHash int64  `json:"accessHash"`
	Title      string `json:"title"`
	CreatedAt  int    `json:"createdAt"`
}

type documentRecord struct {
	ID            int64  `json:"id"`
	AccessHash    int64  `json:"accessHash"`
	FileReference []byte `json:"fileReference"`
	MimeType      string `json:"mimeType"`
	Size          int64  `json:"size"`
	DCID          int    `json:"dcId"`
	FileName      string `json:"fileName"`
	CreatedAt     int    `json:"createdAt"`
}

type messageRecord struct {
	ChannelID  int64 `json:"channelId"`
	ID         int   `json:"id"`
	DocumentID int64 `json:"documentId"`
	CreatedAt  int   `json:"createdAt"`
}

// pendingCommit is the durable intent that ties a staged document blob to the
// document/message indexes before the commit point (state.json) is reached.
// Its presence makes "document published but index not committed" safely
// replayable on restart.
type pendingCommit struct {
	Version      int            `json:"version"`
	ChannelID    int64          `json:"channelId"`
	MessageID    int            `json:"messageId"`
	UploadFileID int64          `json:"uploadFileId"`
	UploadParts  int            `json:"uploadParts"`
	Document     documentRecord `json:"document"`
	CreatedAt    int            `json:"createdAt"`
}

// quarantineManifest is written next to quarantined content so operators can
// tell why the root could not reconcile it automatically.
type quarantineManifest struct {
	Reason     string `json:"reason"`
	DetectedAt string `json:"detectedAt"`
	Source     string `json:"source,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func newState() persistedState {
	return persistedState{
		Version:        stateVersion,
		NextChannelID:  1000,
		NextDocumentID: 10000,
		NextMessageID:  1,
		Channels:       make(map[string]channelRecord),
		Documents:      make(map[string]documentRecord),
		Messages:       make(map[string]messageRecord),
	}
}

func stateBackupPath(statePath string) string { return statePath + stateBackupSuffix }

func decodeState(data []byte) (persistedState, error) {
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return persistedState{}, fmt.Errorf("decode local Telegram state: %w", err)
	}
	if state.Channels == nil {
		state.Channels = make(map[string]channelRecord)
	}
	if state.Documents == nil {
		state.Documents = make(map[string]documentRecord)
	}
	if state.Messages == nil {
		state.Messages = make(map[string]messageRecord)
	}
	if state.NextChannelID <= 0 {
		state.NextChannelID = 1000
	}
	if state.NextDocumentID <= 0 {
		state.NextDocumentID = 10000
	}
	if state.NextMessageID <= 0 {
		state.NextMessageID = 1
	}
	return state, nil
}

// validateVersion rejects indexes from a newer build. Older indexes are
// returned to the caller, which backs the file up before upgrading.
func validateVersion(state persistedState) error {
	if state.Version <= 0 {
		return fmt.Errorf("invalid local Telegram state version %d", state.Version)
	}
	if state.Version > stateVersion {
		return fmt.Errorf("unsupported local Telegram state version %d (maximum supported: %d)", state.Version, stateVersion)
	}
	return nil
}

func pinCounters(state *persistedState) {
	for _, channel := range state.Channels {
		state.NextChannelID = max(state.NextChannelID, channel.ID+1)
	}
	for _, document := range state.Documents {
		state.NextDocumentID = max(state.NextDocumentID, document.ID+1)
	}
	for _, message := range state.Messages {
		state.NextMessageID = max(state.NextMessageID, message.ID+1)
	}
}

func saveState(path string, state persistedState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode local Telegram state: %w", err)
	}
	data = append(data, '\n')
	temp, err := os.CreateTemp(filepath.Dir(path), ".state-*.json")
	if err != nil {
		return fmt.Errorf("create local Telegram state temp file: %w", err)
	}
	tempName := temp.Name()
	cleanup := true
	defer func() {
		_ = temp.Close()
		if cleanup {
			_ = os.Remove(tempName)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod local Telegram state temp file: %w", err)
	}
	if _, err := temp.Write(data); err != nil {
		return fmt.Errorf("write local Telegram state temp file: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync local Telegram state temp file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close local Telegram state temp file: %w", err)
	}
	if err := os.Rename(tempName, path); err != nil {
		return fmt.Errorf("replace local Telegram state: %w", err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync local Telegram state directory: %w", err)
	}
	cleanup = false
	return nil
}

// writeAtomicFile writes payload to path via a temp file in the same
// directory, fsyncs the file and directory, so the rename is durable.
func writeAtomicFile(path string, payload []byte, mode os.FileMode) error {
	if err := writeAtomic(path, payload, mode); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// hashFile returns the hex-encoded SHA-256 digest of a file, matching the
// encoding used for documentRecord.FileReference.
func hashFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

func documentHash(record documentRecord) string {
	return string(record.FileReference)
}

func documentMatchesBlob(record documentRecord, path string) (bool, error) {
	digest, size, err := hashFile(path)
	if err != nil {
		return false, err
	}
	return size == record.Size && digest == documentHash(record), nil
}

// syncDir flushes directory entry changes (renames, unlinks) to disk.
func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}

func replaceFile(source, destination string) error {
	if err := os.Rename(source, destination); err != nil {
		return err
	}
	return syncDir(filepath.Dir(destination))
}

func copyFile(source, destination string, mode os.FileMode) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	syncErr := output.Sync()
	closeErr := output.Close()
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func sameDocument(a, b documentRecord) bool {
	return a.ID == b.ID && a.AccessHash == b.AccessHash && a.Size == b.Size &&
		bytes.Equal(a.FileReference, b.FileReference)
}

func channelKey(id int64) string  { return strconv.FormatInt(id, 10) }
func documentKey(id int64) string { return strconv.FormatInt(id, 10) }
func messageKey(channelID int64, messageID int) string {
	return strconv.FormatInt(channelID, 10) + ":" + strconv.Itoa(messageID)
}
