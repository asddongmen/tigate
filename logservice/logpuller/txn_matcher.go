// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	recordspill "github.com/pingcap/ticdc/pkg/spill"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const (
	prewriteCacheSize             = 16
	clearCacheDelayInSecond       = 5
	prewriteSpillRecordHeaderSize = 24
	prewriteSpillDirName          = "logpuller-prewrite-spill"
)

var (
	prewriteCacheRowNum = metrics.LogPullerPrewriteCacheRowNum
	matcherCount        = metrics.LogPullerMatcherCount
	// The threshold is per region matcher. Keep it small because one large
	// transaction can have unmatched prewrites in many region matchers.
	prewriteSpillMemoryThresholdInBytes = int64(1 * 1024 * 1024)
)

type matchKey struct {
	startTs uint64
	keyHash uint64
	keyLen  uint64
}

func newMatchKey(row *cdcpb.Event_Row) matchKey {
	key := row.GetKey()
	return matchKey{
		startTs: row.GetStartTs(),
		keyHash: xxhash.Sum64(key),
		keyLen:  uint64(len(key)),
	}
}

type prewriteRow struct {
	generation  uint64
	key         []byte
	value       []byte
	oldValue    []byte
	keyLen      int
	valueLen    int
	oldValueLen int
	spillHandle recordspill.Handle
}

type prewritePayload struct {
	key      []byte
	value    []byte
	oldValue []byte
}

func (r *prewriteRow) isSpilled() bool {
	return r.spillHandle.Valid()
}

func (r *prewriteRow) isFakePrewrite() bool {
	return r.valueLen == 0
}

func (r *prewriteRow) bytes() int64 {
	return int64(r.keyLen + r.valueLen + r.oldValueLen)
}

func (r *prewriteRow) loadPayload(spillFile *recordspill.RecordFile) (*prewritePayload, error) {
	if !r.isSpilled() {
		return &prewritePayload{
			key:      r.key,
			value:    r.value,
			oldValue: r.oldValue,
		}, nil
	}
	if spillFile == nil {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("prewrite spill file is missing")
	}

	data, err := spillFile.Read(r.spillHandle)
	if err != nil {
		return nil, err
	}
	if len(data) < prewriteSpillRecordHeaderSize {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("invalid prewrite spill record header")
	}

	keyLen := binary.LittleEndian.Uint64(data[:8])
	valueLen := binary.LittleEndian.Uint64(data[8:16])
	oldValueLen := binary.LittleEndian.Uint64(data[16:prewriteSpillRecordHeaderSize])
	payloadLen := uint64(len(data) - prewriteSpillRecordHeaderSize)
	if keyLen > payloadLen ||
		valueLen > payloadLen ||
		oldValueLen > payloadLen ||
		keyLen+valueLen+oldValueLen != payloadLen {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("invalid prewrite spill record length")
	}

	keyEnd := prewriteSpillRecordHeaderSize + int(keyLen)
	valueEnd := keyEnd + int(valueLen)
	oldValueEnd := valueEnd + int(oldValueLen)
	return &prewritePayload{
		key:      data[prewriteSpillRecordHeaderSize:keyEnd],
		value:    data[keyEnd:valueEnd],
		oldValue: data[valueEnd:oldValueEnd],
	}, nil
}

type matcher struct {
	unmatchedValue     map[matchKey]prewriteRow
	cachedCommit       []*cdcpb.Event_Row
	cachedRollback     []*cdcpb.Event_Row
	lastPrewriteTime   time.Time
	inMemoryValueBytes int64
	spilledPrewriteNum int
	spillFile          *recordspill.RecordFile
}

func newMatcher() *matcher {
	matcherCount.Inc()
	return &matcher{
		unmatchedValue: make(map[matchKey]prewriteRow, prewriteCacheSize),
	}
}

func (m *matcher) putPrewriteRow(row *cdcpb.Event_Row) {
	key := newMatchKey(row)
	if old, exist := m.unmatchedValue[key]; exist {
		if !m.prewriteKeyMatches(&old, row) {
			log.Panic("prewrite row key hash collision",
				zap.Uint64("startTs", row.GetStartTs()),
				zap.String("key", util.RedactKey(row.GetKey())))
		}
		// tikv may send a fake prewrite event with empty value caused by txn heartbeat.
		// here we need to avoid the fake prewrite event overwrite the prewrite value.

		// when the old-value is disabled, the value of the fake prewrite event is empty.
		// when the old-value is enabled, the value of the fake prewrite event is also empty,
		// but the old value of the fake prewrite event is not empty.
		// We can distinguish fake prewrite events by whether the value is empty,
		// no matter the old-value is enabled or disabled
		if len(row.GetValue()) == 0 {
			return
		}

		// For pipelined-DML transactions, the row with latest Generation will be kept.
		if row.Generation < old.generation {
			return
		}
		m.removePrewriteRow(key)
	}
	if m.unmatchedValue == nil {
		m.unmatchedValue = make(map[matchKey]prewriteRow, prewriteCacheSize)
	}
	prewrite, err := m.newPrewriteRow(row)
	if err != nil {
		log.Panic("failed to store prewrite row",
			zap.Uint64("startTs", row.GetStartTs()),
			zap.String("key", util.RedactKey(row.GetKey())),
			zap.Error(err))
	}
	m.unmatchedValue[key] = *prewrite
	m.lastPrewriteTime = time.Now()
	prewriteCacheRowNum.Inc()
}

// matchRow matches the commit event with the cached prewrite event
// the Value and OldValue will be assigned if a matched prewrite event exists.
func (m *matcher) matchRow(row *cdcpb.Event_Row, initialized bool) bool {
	value, payload, exist := m.popPrewriteRow(row, initialized)
	if !exist {
		return false
	}
	row.Value = payload.value
	row.OldValue = payload.oldValue
	m.releasePrewriteValue(value)
	m.cleanupSpillFileIfUnused()
	return true
}

func (m *matcher) matchRowWithoutValue(row *cdcpb.Event_Row, initialized bool) bool {
	value, _, exist := m.popPrewriteRow(row, initialized)
	if exist {
		m.releasePrewriteValue(value)
	}
	m.cleanupSpillFileIfUnused()
	return exist
}

func (m *matcher) popPrewriteRow(
	row *cdcpb.Event_Row,
	initialized bool,
) (*prewriteRow, *prewritePayload, bool) {
	key := newMatchKey(row)
	if value, exist := m.unmatchedValue[key]; exist {
		payload, err := value.loadPayload(m.spillFile)
		if err != nil {
			log.Panic("failed to read prewrite row",
				zap.Uint64("startTs", row.GetStartTs()),
				zap.Uint64("commitTs", row.GetCommitTs()),
				zap.String("key", util.RedactKey(row.GetKey())),
				zap.Error(err))
		}
		if !bytes.Equal(payload.key, row.GetKey()) {
			log.Panic("prewrite row key hash collision",
				zap.Uint64("startTs", row.GetStartTs()),
				zap.Uint64("commitTs", row.GetCommitTs()),
				zap.String("key", util.RedactKey(row.GetKey())))
		}
		// TiKV may send a fake prewrite event with empty value caused by txn heartbeat.
		//
		// We need to skip match if the region is not initialized,
		// as prewrite events may be sent out of order.
		if !initialized && value.isFakePrewrite() {
			return nil, nil, false
		}
		// Pipelined-DML transactions can only be matched after initialized.
		if !initialized && value.generation > 0 {
			return nil, nil, false
		}
		delete(m.unmatchedValue, key)
		prewriteCacheRowNum.Dec()
		return &value, payload, true
	}
	return nil, nil, false
}

func (m *matcher) cacheCommitRow(row *cdcpb.Event_Row) {
	m.cachedCommit = append(m.cachedCommit, row)
}

//nolint:unparam
func (m *matcher) matchCachedRow(initialized bool) []*cdcpb.Event_Row {
	return m.matchCachedRowWithFilter(initialized, nil)
}

func (m *matcher) matchCachedRowWithFilter(
	initialized bool,
	shouldEmit func(row *cdcpb.Event_Row) bool,
) []*cdcpb.Event_Row {
	if !initialized {
		log.Panic("must be initialized before match cached rows")
	}
	cachedCommit := m.cachedCommit
	m.cachedCommit = nil
	top := 0
	for i := 0; i < len(cachedCommit); i++ {
		cacheEntry := cachedCommit[i]
		emit := shouldEmit == nil || shouldEmit(cacheEntry)
		ok := false
		if emit {
			ok = m.matchRow(cacheEntry, true)
		} else {
			ok = m.matchRowWithoutValue(cacheEntry, true)
		}
		if !ok {
			// when cdc receives a commit log without a corresponding
			// prewrite log before initialized, a committed log  with
			// the same key and start-ts must have been received.
			log.Info("ignore commit event without prewrite",
				zap.String("key", util.RedactKey(cacheEntry.GetKey())),
				zap.Uint64("startTs", cacheEntry.GetStartTs()))
			continue
		}
		if !emit {
			continue
		}
		cachedCommit[top] = cacheEntry
		top++
	}
	return cachedCommit[:top]
}

func (m *matcher) rollbackRow(row *cdcpb.Event_Row) {
	key := newMatchKey(row)
	value, exist := m.unmatchedValue[key]
	if !exist {
		return
	}
	if !m.prewriteKeyMatches(&value, row) {
		log.Panic("prewrite row key hash collision",
			zap.Uint64("startTs", row.GetStartTs()),
			zap.String("key", util.RedactKey(row.GetKey())))
	}
	delete(m.unmatchedValue, key)
	m.releasePrewriteValue(&value)
	m.cleanupSpillFileIfUnused()
	prewriteCacheRowNum.Dec()
}

func (m *matcher) cacheRollbackRow(row *cdcpb.Event_Row) {
	m.cachedRollback = append(m.cachedRollback, row)
}

//nolint:unparam
func (m *matcher) matchCachedRollbackRow(initialized bool) {
	if !initialized {
		log.Panic("must be initialized before match cached rollback rows")
	}
	rollback := m.cachedRollback
	m.cachedRollback = nil
	for i := 0; i < len(rollback); i++ {
		cacheEntry := rollback[i]
		m.rollbackRow(cacheEntry)
	}
}

func (m *matcher) tryCleanUnmatchedValue() {
	if m.unmatchedValue == nil {
		return
	}
	// Only clear the unmatched value if it has been 10 seconds since the last prewrite event
	// and there is no unmatched value left.
	if time.Since(m.lastPrewriteTime) > clearCacheDelayInSecond*time.Second && len(m.unmatchedValue) == 0 {
		m.clearUnmatchedValue()
	}
}

func (m *matcher) clearUnmatchedValue() {
	m.lastPrewriteTime = time.Time{}
	for k := range m.unmatchedValue {
		delete(m.unmatchedValue, k)
	}
	m.unmatchedValue = nil
	m.inMemoryValueBytes = 0
	m.spilledPrewriteNum = 0
	if m.spillFile != nil {
		if err := m.spillFile.Cleanup(); err != nil {
			log.Warn("failed to cleanup prewrite spill file", zap.Error(err))
		}
		m.spillFile = nil
	}
}

func (m *matcher) clear() {
	matcherCount.Dec()
	prewriteCacheRowNum.Sub(float64(len(m.unmatchedValue)))
	m.clearUnmatchedValue()
	m.cachedCommit = nil
	m.cachedRollback = nil
}

func (m *matcher) newPrewriteRow(row *cdcpb.Event_Row) (*prewriteRow, error) {
	keyLen := len(row.GetKey())
	valueLen := len(row.GetValue())
	oldValueLen := len(row.GetOldValue())
	recordBytes := int64(keyLen + valueLen + oldValueLen)
	if !m.shouldSpillPrewriteRow(recordBytes) {
		m.inMemoryValueBytes += recordBytes
		return &prewriteRow{
			generation:  row.Generation,
			key:         row.GetKey(),
			value:       row.GetValue(),
			oldValue:    row.GetOldValue(),
			keyLen:      keyLen,
			valueLen:    valueLen,
			oldValueLen: oldValueLen,
		}, nil
	}

	if err := m.ensureSpillFile(); err != nil {
		return nil, err
	}
	var header [prewriteSpillRecordHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(keyLen))
	binary.LittleEndian.PutUint64(header[8:16], uint64(valueLen))
	binary.LittleEndian.PutUint64(header[16:], uint64(oldValueLen))
	handle, err := m.spillFile.AppendChunks(header[:], row.GetKey(), row.GetValue(), row.GetOldValue())
	if err != nil {
		return nil, err
	}
	row.Key = nil
	row.Value = nil
	row.OldValue = nil
	m.spilledPrewriteNum++
	return &prewriteRow{
		generation:  row.Generation,
		keyLen:      keyLen,
		valueLen:    valueLen,
		oldValueLen: oldValueLen,
		spillHandle: handle,
	}, nil
}

func (m *matcher) shouldSpillPrewriteRow(recordBytes int64) bool {
	if recordBytes == 0 {
		return false
	}
	if prewriteSpillMemoryThresholdInBytes <= 0 {
		return true
	}
	return m.spillFile != nil ||
		m.inMemoryValueBytes+recordBytes > prewriteSpillMemoryThresholdInBytes
}

func (m *matcher) ensureSpillFile() error {
	if m.spillFile != nil {
		return nil
	}
	dataDir := config.GetGlobalServerConfig().DataDir
	if dataDir == "" {
		dataDir = os.TempDir()
	}
	spillFile, err := recordspill.NewRecordFile(
		filepath.Join(dataDir, prewriteSpillDirName),
		"logpuller-prewrite-*.spill",
	)
	if err != nil {
		return err
	}
	m.spillFile = spillFile
	return nil
}

func (m *matcher) removePrewriteRow(key matchKey) {
	value, exist := m.unmatchedValue[key]
	if !exist {
		return
	}
	delete(m.unmatchedValue, key)
	m.releasePrewriteValue(&value)
	m.cleanupSpillFileIfUnused()
	prewriteCacheRowNum.Dec()
}

func (m *matcher) releasePrewriteValue(value *prewriteRow) {
	if value == nil {
		return
	}
	if value.isSpilled() {
		if m.spilledPrewriteNum > 0 {
			m.spilledPrewriteNum--
		}
		return
	}
	m.inMemoryValueBytes -= value.bytes()
	if m.inMemoryValueBytes < 0 {
		m.inMemoryValueBytes = 0
	}
}

func (m *matcher) prewriteKeyMatches(value *prewriteRow, row *cdcpb.Event_Row) bool {
	payload, err := value.loadPayload(m.spillFile)
	if err != nil {
		log.Panic("failed to read prewrite row",
			zap.Uint64("startTs", row.GetStartTs()),
			zap.String("key", util.RedactKey(row.GetKey())),
			zap.Error(err))
	}
	return bytes.Equal(payload.key, row.GetKey())
}

func (m *matcher) cleanupSpillFileIfUnused() {
	if m.spilledPrewriteNum > 0 || m.spillFile == nil {
		return
	}
	if err := m.spillFile.Cleanup(); err != nil {
		log.Warn("failed to cleanup prewrite spill file", zap.Error(err))
	}
	m.spillFile = nil
}
