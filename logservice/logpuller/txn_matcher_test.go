// Copyright 2024 PingCAP, Inc.
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
	"os"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestMatchRow(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
		Value:   []byte("v1"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  2,
		Key:      []byte("k1"),
		Value:    []byte("v2"),
		OldValue: []byte("v3"),
	})

	// test rollback
	matcher.rollbackRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	})
	commitRow1 := &cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	}
	ok := matcher.matchRow(commitRow1, true)
	require.False(t, ok)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	}, commitRow1)

	// test match commit
	commitRow2 := &cdcpb.Event_Row{
		StartTs:  2,
		CommitTs: 3,
		Key:      []byte("k1"),
	}
	ok = matcher.matchRow(commitRow2, true)
	require.True(t, ok)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs:  2,
		CommitTs: 3,
		Key:      []byte("k1"),
		Value:    []byte("v2"),
		OldValue: []byte("v3"),
	}, commitRow2)
}

func TestMatchFakePrewrite(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("v3"),
	})
	// fake prewrite
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		OldValue: []byte("v4"),
	})

	commitRow1 := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}
	ok := matcher.matchRow(commitRow1, true)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("v3"),
	}, commitRow1)
	require.True(t, ok)
}

func TestMatchRowWithoutValue(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("ov1"),
	})

	commitRow := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}
	require.True(t, matcher.matchRowWithoutValue(commitRow, true))
	require.Nil(t, commitRow.Value)
	require.Nil(t, commitRow.OldValue)
	require.Empty(t, matcher.unmatchedValue)
}

func TestMatcherSpillsPrewriteRowsOverThreshold(t *testing.T) {
	withPrewriteSpillTestConfig(t, 1)

	matcher := newMatcher()
	prewrite := &cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("ov1"),
	}
	matcher.putPrewriteRow(prewrite)

	require.Nil(t, prewrite.Key)
	require.Nil(t, prewrite.Value)
	require.Nil(t, prewrite.OldValue)
	stored := matcher.unmatchedValue[newMatchKey(&cdcpb.Event_Row{StartTs: 1, Key: []byte("k1")})]
	require.True(t, stored.isSpilled())
	require.Nil(t, stored.key)
	require.Zero(t, matcher.inMemoryValueBytes)
	require.Equal(t, 1, matcher.spilledPrewriteNum)
	spillPath := matcher.spillFile.Path()

	commitRow := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}
	require.True(t, matcher.matchRow(commitRow, true))
	require.Equal(t, []byte("v1"), commitRow.Value)
	require.Equal(t, []byte("ov1"), commitRow.OldValue)
	require.Empty(t, matcher.unmatchedValue)
	require.Zero(t, matcher.spilledPrewriteNum)
	require.Nil(t, matcher.spillFile)

	_, err := os.Stat(spillPath)
	require.True(t, os.IsNotExist(err))
}

func TestMatcherSpillsSmallRowsAfterMemoryThreshold(t *testing.T) {
	withPrewriteSpillTestConfig(t, 5)

	matcher := newMatcher()
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
		Value:   []byte("aa"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs: 2,
		Key:     []byte("k2"),
		Value:   []byte("bb"),
	})

	stored1 := matcher.unmatchedValue[newMatchKey(&cdcpb.Event_Row{StartTs: 1, Key: []byte("k1")})]
	stored2 := matcher.unmatchedValue[newMatchKey(&cdcpb.Event_Row{StartTs: 2, Key: []byte("k2")})]
	require.False(t, stored1.isSpilled())
	require.True(t, stored2.isSpilled())
	require.Equal(t, int64(4), matcher.inMemoryValueBytes)

	commitRow1 := &cdcpb.Event_Row{StartTs: 1, CommitTs: 3, Key: []byte("k1")}
	require.True(t, matcher.matchRow(commitRow1, true))
	require.Equal(t, []byte("aa"), commitRow1.Value)
	require.Zero(t, matcher.inMemoryValueBytes)
	require.NotNil(t, matcher.spillFile)

	commitRow2 := &cdcpb.Event_Row{StartTs: 2, CommitTs: 4, Key: []byte("k2")}
	require.True(t, matcher.matchRow(commitRow2, true))
	require.Equal(t, []byte("bb"), commitRow2.Value)
	require.Nil(t, matcher.spillFile)
}

func TestMatcherStaleCommitDoesNotReadSpilledValue(t *testing.T) {
	withPrewriteSpillTestConfig(t, 1)

	matcher := newMatcher()
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("ov1"),
	})
	spillPath := matcher.spillFile.Path()

	commitRow := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}
	require.True(t, matcher.matchRowWithoutValue(commitRow, true))
	require.Nil(t, commitRow.Value)
	require.Nil(t, commitRow.OldValue)
	require.Empty(t, matcher.unmatchedValue)
	require.Nil(t, matcher.spillFile)

	_, err := os.Stat(spillPath)
	require.True(t, os.IsNotExist(err))
}

func TestMatcherRollbackCleansSpilledPrewrite(t *testing.T) {
	withPrewriteSpillTestConfig(t, 1)

	matcher := newMatcher()
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
		Value:   []byte("v1"),
	})
	spillPath := matcher.spillFile.Path()

	matcher.rollbackRow(&cdcpb.Event_Row{StartTs: 1, Key: []byte("k1")})
	require.Empty(t, matcher.unmatchedValue)
	require.Zero(t, matcher.spilledPrewriteNum)
	require.Nil(t, matcher.spillFile)

	_, err := os.Stat(spillPath)
	require.True(t, os.IsNotExist(err))
}

func TestMatchRowUninitialized(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()

	// fake prewrite before init.
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		OldValue: []byte("v4"),
	})
	commitRow1 := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}
	ok := matcher.matchRow(commitRow1, false)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}, commitRow1)
	require.False(t, ok)
	matcher.cacheCommitRow(commitRow1)

	// actual prewrite before init.
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v3"),
		OldValue: []byte("v4"),
	})

	// normal prewrite and commit before init.
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  2,
		Key:      []byte("k2"),
		Value:    []byte("v3"),
		OldValue: []byte("v4"),
	})
	commitRow2 := &cdcpb.Event_Row{
		StartTs:  2,
		CommitTs: 3,
		Key:      []byte("k2"),
	}
	ok = matcher.matchRow(commitRow2, false)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs:  2,
		CommitTs: 3,
		Key:      []byte("k2"),
		Value:    []byte("v3"),
		OldValue: []byte("v4"),
	}, commitRow2)
	require.True(t, ok)

	// match cached row after init.
	rows := matcher.matchCachedRow(true)
	require.Len(t, rows, 1)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
		Value:    []byte("v3"),
		OldValue: []byte("v4"),
	}, rows[0])
}

func TestMatchMatchCachedRow(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()
	require.Equal(t, 0, len(matcher.matchCachedRow(true)))
	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	})
	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  3,
		CommitTs: 4,
		Key:      []byte("k2"),
	})
	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  4,
		CommitTs: 5,
		Key:      []byte("k3"),
	})
	require.Equal(t, 0, len(matcher.matchCachedRow(true)))

	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	})
	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  3,
		CommitTs: 4,
		Key:      []byte("k2"),
	})
	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  4,
		CommitTs: 5,
		Key:      []byte("k3"),
	})

	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("ov1"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  3,
		Key:      []byte("k2"),
		Value:    []byte("v2"),
		OldValue: []byte("ov2"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  4,
		Key:      []byte("k2"),
		Value:    []byte("v3"),
		OldValue: []byte("ov3"),
	})

	require.Equal(t, []*cdcpb.Event_Row{{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("ov1"),
	}, {
		StartTs:  3,
		CommitTs: 4,
		Key:      []byte("k2"),
		Value:    []byte("v2"),
		OldValue: []byte("ov2"),
	}}, matcher.matchCachedRow(true))
}

func TestMatchCachedRowWithFilterDoesNotFillSkippedRows(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()
	cachedCommit := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}
	matcher.cacheCommitRow(cachedCommit)
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("ov1"),
	})

	rows := matcher.matchCachedRowWithFilter(true, func(row *cdcpb.Event_Row) bool {
		return row.GetCommitTs() > 2
	})
	require.Empty(t, rows)
	require.Nil(t, cachedCommit.Value)
	require.Nil(t, cachedCommit.OldValue)
	require.Empty(t, matcher.unmatchedValue)
}

func TestMatchMatchCachedRollbackRow(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()
	matcher.matchCachedRollbackRow(true)
	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	})
	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 3,
		Key:     []byte("k2"),
	})
	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 4,
		Key:     []byte("k3"),
	})
	matcher.matchCachedRollbackRow(true)

	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	})
	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 3,
		Key:     []byte("k2"),
	})
	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 4,
		Key:     []byte("k3"),
	})

	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("ov1"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  3,
		Key:      []byte("k2"),
		Value:    []byte("v2"),
		OldValue: []byte("ov2"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  4,
		Key:      []byte("k3"),
		Value:    []byte("v3"),
		OldValue: []byte("ov3"),
	})

	matcher.matchCachedRollbackRow(true)
	require.Empty(t, matcher.unmatchedValue)
}

func TestMatcher_TryCleanUnmatchedValue(t *testing.T) {
	tests := []struct {
		name         string
		setupMatcher func() *matcher
		wait         time.Duration
		wantNilMap   bool
	}{
		{
			name: "nil map should remain nil",
			setupMatcher: func() *matcher {
				m := newMatcher()
				m.unmatchedValue = nil
				return m
			},
			wait:       6 * time.Second,
			wantNilMap: true,
		},
		{
			name: "should not clean when time not reached",
			setupMatcher: func() *matcher {
				m := newMatcher()
				m.lastPrewriteTime = time.Now()
				return m
			},
			wait:       1 * time.Second,
			wantNilMap: false,
		},
		{
			name: "should not clean when has values",
			setupMatcher: func() *matcher {
				m := newMatcher()
				m.lastPrewriteTime = time.Now()
				m.unmatchedValue[newMatchKey(&cdcpb.Event_Row{StartTs: 1, Key: []byte("test")})] = prewriteRow{}
				return m
			},
			wait:       6 * time.Second,
			wantNilMap: false,
		},
		{
			name: "should clean when time reached and empty",
			setupMatcher: func() *matcher {
				m := newMatcher()
				m.lastPrewriteTime = time.Now()
				return m
			},
			wait:       6 * time.Second,
			wantNilMap: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.setupMatcher()
			time.Sleep(tt.wait)
			m.tryCleanUnmatchedValue()

			if tt.wantNilMap {
				require.Nil(t, m.unmatchedValue)
			} else {
				require.NotNil(t, m.unmatchedValue)
			}
		})
	}
}

func TestMatchPipelinedDMLs(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()

	matcher.putPrewriteRow(&cdcpb.Event_Row{
		Generation: 2,
		StartTs:    1,
		Key:        []byte("k"),
		Value:      []byte("v2"),
		OldValue:   []byte("ov2"),
	})
	row := &cdcpb.Event_Row{StartTs: 1, CommitTs: 3, Key: []byte("k")}
	matched := matcher.matchRow(row, false)
	require.False(t, matched, "prewrites with generation shouldn't be matched before initialized")

	matcher.cacheCommitRow(row)

	matcher.putPrewriteRow(&cdcpb.Event_Row{
		Generation: 4,
		StartTs:    1,
		Key:        []byte("k"),
		Value:      []byte("v4"),
		OldValue:   []byte("ov4"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		Generation: 3,
		StartTs:    1,
		Key:        []byte("k"),
		Value:      []byte("v3"),
		OldValue:   []byte("ov3"),
	})

	rows := matcher.matchCachedRow(true)
	require.Equal(t, 1, len(rows))
	require.Equal(t, rows[0].Value, []byte("v4"))
}

func withPrewriteSpillTestConfig(t *testing.T, threshold int64) {
	t.Helper()

	oldThreshold := prewriteSpillMemoryThresholdInBytes
	prewriteSpillMemoryThresholdInBytes = threshold

	originalConfig := config.GetGlobalServerConfig().Clone()
	cfg := originalConfig.Clone()
	cfg.DataDir = t.TempDir()
	config.StoreGlobalServerConfig(cfg)

	t.Cleanup(func() {
		prewriteSpillMemoryThresholdInBytes = oldThreshold
		config.StoreGlobalServerConfig(originalConfig)
	})
}
