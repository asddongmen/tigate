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
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func requireClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	default:
		require.Fail(t, "channel is not closed")
	}
}

// For UPDATE SQL, its prewrite event has both value and old value.
// It is possible that TiDB prewrites multiple times for the same row when
// there are other transactions it conflicts with. For this case,
// if the value is not "short", only the first prewrite contains the value.
//
// TiKV may output events for the UPDATE SQL as following:
//
// TiDB: [Prwrite1]    [Prewrite2]      [Commit]
//
//	v             v                v                                   Time
//
// ---------------------------------------------------------------------------->
//
//	^            ^    ^           ^     ^       ^     ^          ^     ^
//
// TiKV:   [Scan Start] [Send Prewrite2] [Send Commit] [Send Prewrite1] [Send Init]
// TiCDC:                    [Recv Prewrite2]  [Recv Commit] [Recv Prewrite1] [Recv Init]
func TestHandleEventEntryEventOutOfOrder(t *testing.T) {
	// initialize
	option := dynstream.NewOption()
	ds := dynstream.NewParallelDynamicStream("test", &regionEventHandler{}, option)
	ds.Start()

	span := heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: common.ToComparableKey([]byte{}), // TODO: remove spanz dependency
		EndKey:   common.ToComparableKey(common.UpperBoundKey),
	}
	subID := SubscriptionID(999)
	eventCh := make(chan common.RawKVEntry, 1000)
	consumeKVEvents := func(events []common.RawKVEntry, _ func()) bool {
		for _, e := range events {
			eventCh <- e
		}
		return false
	}
	advanceResolvedTs := func(ts uint64) {
		// not used
	}
	subSpan := &subscribedSpan{
		subID:             subID,
		span:              span,
		startTs:           0,
		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
		advanceInterval:   0,
	}
	ds.AddPath(subID, subSpan, dynstream.AreaSettings{})

	worker := &regionRequestWorker{
		requestCache: &requestCache{},
	}
	region := newRegionInfo(
		tikv.RegionVerID{},
		span,
		&tikv.RPCContext{},
		subSpan,
		false,
	)
	region.lockedRangeState = &regionlock.LockedRangeState{}
	state := newRegionFeedState(region, 1, worker)
	state.start()

	// Receive prewrite2 with empty value.
	{
		events := &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{{
					StartTs:  1,
					Type:     cdcpb.Event_PREWRITE,
					OpType:   cdcpb.Event_Row_PUT,
					Key:      []byte("key"),
					Value:    nil,
					OldValue: []byte("oldvalue"),
				}},
			},
		}
		regionEvent := regionEvent{
			states:  []*regionFeedState{state},
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Receive commit.
	{
		events := &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{{
					StartTs:  1,
					CommitTs: 2,
					Type:     cdcpb.Event_COMMIT,
					OpType:   cdcpb.Event_Row_PUT,
					Key:      []byte("key"),
				}},
			},
		}
		regionEvent := regionEvent{
			states:  []*regionFeedState{state},
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Must not output event.
	{
		select {
		case <-eventCh:
			require.True(t, false, "shouldn't get an event")
		case <-time.NewTimer(100 * time.Millisecond).C:
		}
	}

	// Receive prewrite1 with actual value.
	{
		events := &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{{
					StartTs:  1,
					Type:     cdcpb.Event_PREWRITE,
					OpType:   cdcpb.Event_Row_PUT,
					Key:      []byte("key"),
					Value:    []byte("value"),
					OldValue: []byte("oldvalue"),
				}},
			},
		}
		regionEvent := regionEvent{
			states:  []*regionFeedState{state},
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Must not output event.
	{
		select {
		case <-eventCh:
			require.True(t, false, "shouldn't get an event")
		case <-time.NewTimer(100 * time.Millisecond).C:
		}
	}

	// Receive initialized.
	{
		events := &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{
					{
						Type: cdcpb.Event_INITIALIZED,
					},
				},
			},
		}
		regionEvent := regionEvent{
			states:  []*regionFeedState{state},
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Must output event.
	{
		select {
		case event := <-eventCh:
			require.Equal(t, uint64(2), event.CRTs)
			require.Equal(t, uint64(1), event.StartTs)
			require.Equal(t, "value", string(event.Value))
			require.Equal(t, "oldvalue", string(event.OldValue))
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}
}

func TestHandleEventEntriesSkipsStaleCommitValue(t *testing.T) {
	span := heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: common.ToComparableKey([]byte{}),
		EndKey:   common.ToComparableKey(common.UpperBoundKey),
	}
	subSpan := &subscribedSpan{
		subID:   SubscriptionID(1),
		span:    span,
		startTs: 10,
	}
	region := newRegionInfo(
		tikv.NewRegionVerID(1, 1, 1),
		span,
		&tikv.RPCContext{},
		subSpan,
		false,
	)
	region.lockedRangeState = &regionlock.LockedRangeState{}
	region.lockedRangeState.Initialized.Store(true)
	state := newRegionFeedState(region, uint64(subSpan.subID), &regionRequestWorker{requestCache: &requestCache{}})
	state.start()

	commit := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 10,
		Type:     cdcpb.Event_COMMIT,
		OpType:   cdcpb.Event_Row_PUT,
		Key:      []byte("key"),
	}
	handleEventEntries(subSpan, state, &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{
				{
					StartTs: 1,
					Type:    cdcpb.Event_PREWRITE,
					OpType:  cdcpb.Event_Row_PUT,
					Key:     []byte("key"),
					Value:   []byte("value"),
				},
				commit,
			},
		},
	}, func() {})

	require.Empty(t, subSpan.kvEventsCache)
	require.Nil(t, commit.Value)
	require.Nil(t, commit.OldValue)
	require.Empty(t, state.matcher.unmatchedValue)
}

func TestHandleEventEntriesSkipsCachedStaleCommitAfterInitialized(t *testing.T) {
	span := heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: common.ToComparableKey([]byte{}),
		EndKey:   common.ToComparableKey(common.UpperBoundKey),
	}
	subSpan := &subscribedSpan{
		subID:   SubscriptionID(1),
		span:    span,
		startTs: 10,
	}
	region := newRegionInfo(
		tikv.NewRegionVerID(1, 1, 1),
		span,
		&tikv.RPCContext{},
		subSpan,
		false,
	)
	region.lockedRangeState = &regionlock.LockedRangeState{}
	state := newRegionFeedState(region, uint64(subSpan.subID), &regionRequestWorker{requestCache: &requestCache{}})
	state.start()

	commit := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 10,
		Type:     cdcpb.Event_COMMIT,
		OpType:   cdcpb.Event_Row_PUT,
		Key:      []byte("key"),
	}
	handleEventEntries(subSpan, state, &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{Entries: []*cdcpb.Event_Row{commit}},
	}, func() {})
	handleEventEntries(subSpan, state, &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{
				{
					StartTs: 1,
					Type:    cdcpb.Event_PREWRITE,
					OpType:  cdcpb.Event_Row_PUT,
					Key:     []byte("key"),
					Value:   []byte("value"),
				},
				{Type: cdcpb.Event_INITIALIZED},
			},
		},
	}, func() {})

	require.Empty(t, subSpan.kvEventsCache)
	require.Nil(t, commit.Value)
	require.Nil(t, commit.OldValue)
	require.Empty(t, state.matcher.unmatchedValue)
}

func TestHandleEventEntriesFlushesLargeKVEventCache(t *testing.T) {
	oldThreshold := kvEventsCacheFlushThresholdInBytes
	oldGCThreshold := kvEventsCacheGCThresholdInBytes
	oldForceGC := forceGC
	kvEventsCacheFlushThresholdInBytes = 1
	kvEventsCacheGCThresholdInBytes = 1
	gcCount := 0
	forceGC = func() {
		gcCount++
	}
	t.Cleanup(func() {
		kvEventsCacheFlushThresholdInBytes = oldThreshold
		kvEventsCacheGCThresholdInBytes = oldGCThreshold
		forceGC = oldForceGC
	})

	span := heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: common.ToComparableKey([]byte{}),
		EndKey:   common.ToComparableKey(common.UpperBoundKey),
	}
	var chunks [][]common.RawKVEntry
	subSpan := &subscribedSpan{
		subID:   SubscriptionID(1),
		span:    span,
		startTs: 0,
		consumeKVEvents: func(events []common.RawKVEntry, _ func()) bool {
			copied := append([]common.RawKVEntry(nil), events...)
			chunks = append(chunks, copied)
			return false
		},
		advanceResolvedTs: func(uint64) {},
	}
	region := newRegionInfo(
		tikv.NewRegionVerID(1, 1, 1),
		span,
		&tikv.RPCContext{},
		subSpan,
		false,
	)
	region.lockedRangeState = &regionlock.LockedRangeState{}
	region.lockedRangeState.Initialized.Store(true)
	state := newRegionFeedState(region, uint64(subSpan.subID), &regionRequestWorker{requestCache: &requestCache{}})
	state.start()

	rows := make([]*cdcpb.Event_Row, 0, 6)
	for i := 0; i < 3; i++ {
		startTs := uint64(i + 1)
		key := []byte{byte('a' + i)}
		rows = append(rows,
			&cdcpb.Event_Row{
				StartTs: startTs,
				Type:    cdcpb.Event_PREWRITE,
				OpType:  cdcpb.Event_Row_PUT,
				Key:     key,
				Value:   []byte{byte('A' + i)},
			},
			&cdcpb.Event_Row{
				StartTs:  startTs,
				CommitTs: 10,
				Type:     cdcpb.Event_COMMIT,
				OpType:   cdcpb.Event_Row_PUT,
				Key:      key,
			},
		)
	}

	handler := &regionEventHandler{}
	processed := make(chan struct{})
	await := handler.Handle(subSpan, regionEvent{
		states:    []*regionFeedState{state},
		processed: processed,
		entries: &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{Entries: rows},
		},
	})

	require.False(t, await)
	requireClosed(t, processed)
	require.Len(t, chunks, 3)
	for i, chunk := range chunks {
		require.Len(t, chunk, 1)
		require.Equal(t, []byte{byte('a' + i)}, chunk[0].Key)
		require.Equal(t, []byte{byte('A' + i)}, chunk[0].Value)
	}
	for _, row := range rows {
		require.Nil(t, row.Value)
		require.Nil(t, row.OldValue)
	}
	require.Empty(t, subSpan.kvEventsCache)
	require.Zero(t, subSpan.kvEventsCacheBytes)
	require.Equal(t, 3, gcCount)
}

func TestHandleResolvedTs(t *testing.T) {
	// initialize
	option := dynstream.NewOption()
	ds := dynstream.NewParallelDynamicStream("test", &regionEventHandler{}, option)
	ds.Start()

	consumeKVEvents := func(events []common.RawKVEntry, _ func()) bool { return false } // not used
	tsCh := make(chan uint64, 100)
	advanceResolvedTs := func(ts uint64) {
		tsCh <- ts
	}

	subID1 := SubscriptionID(1)
	worker := &regionRequestWorker{
		requestCache: &requestCache{},
	}
	state1 := newRegionFeedState(regionInfo{verID: tikv.NewRegionVerID(1, 1, 1)}, uint64(subID1), worker)
	state1.start()
	{
		span := heartbeatpb.TableSpan{
			TableID:  100,
			StartKey: common.ToComparableKey([]byte{}), // TODO: remove spanz dependency
			EndKey:   common.ToComparableKey(common.UpperBoundKey),
		}
		subSpan := &subscribedSpan{
			subID:             subID1,
			span:              heartbeatpb.TableSpan{},
			rangeLock:         regionlock.NewRangeLock(uint64(subID1), span.StartKey, span.EndKey, 1),
			consumeKVEvents:   consumeKVEvents,
			advanceResolvedTs: advanceResolvedTs,
			advanceInterval:   0,
		}
		ds.AddPath(subID1, subSpan, dynstream.AreaSettings{})
		state1.region.subscribedSpan = subSpan
		state1.region.lockedRangeState = &regionlock.LockedRangeState{}
		state1.setInitialized()
		state1.updateResolvedTs(9)
	}

	subID2 := SubscriptionID(2)
	state2 := newRegionFeedState(regionInfo{verID: tikv.NewRegionVerID(2, 2, 2)}, uint64(subID2), worker)
	state2.start()
	{
		span := heartbeatpb.TableSpan{
			TableID:  100,
			StartKey: common.ToComparableKey([]byte{}), // TODO: remove spanz dependency
			EndKey:   common.ToComparableKey(common.UpperBoundKey),
		}
		subSpan := &subscribedSpan{
			subID:             subID2,
			span:              span,
			rangeLock:         regionlock.NewRangeLock(uint64(subID2), span.StartKey, span.EndKey, 1),
			consumeKVEvents:   consumeKVEvents,
			advanceResolvedTs: advanceResolvedTs,
			advanceInterval:   0,
		}
		ds.AddPath(subID2, subSpan, dynstream.AreaSettings{})
		state2.region.subscribedSpan = subSpan
		state2.region.lockedRangeState = &regionlock.LockedRangeState{}
		state2.setInitialized()
		state2.updateResolvedTs(11)
	}

	subID3 := SubscriptionID(3)
	state3 := newRegionFeedState(regionInfo{verID: tikv.NewRegionVerID(3, 3, 3)}, uint64(subID3), worker)
	state3.start()
	{
		span := heartbeatpb.TableSpan{
			TableID:  100,
			StartKey: common.ToComparableKey([]byte{}), // TODO: remove spanz dependency
			EndKey:   common.ToComparableKey(common.UpperBoundKey),
		}
		subSpan := &subscribedSpan{
			subID:             subID3,
			span:              span,
			rangeLock:         regionlock.NewRangeLock(uint64(subID3), span.StartKey, span.EndKey, 1),
			consumeKVEvents:   consumeKVEvents,
			advanceResolvedTs: advanceResolvedTs,
			advanceInterval:   0,
		}
		ds.AddPath(subID3, subSpan, dynstream.AreaSettings{})
		state3.region.subscribedSpan = subSpan
		state3.region.lockedRangeState = &regionlock.LockedRangeState{}
		state3.updateResolvedTs(8)
	}

	{
		regionEvent := regionEvent{
			resolvedTs: 10,
			states:     []*regionFeedState{state1},
		}
		ds.Push(subID1, regionEvent)
	}
	{
		regionEvent := regionEvent{
			resolvedTs: 10,
			states:     []*regionFeedState{state2},
		}
		ds.Push(subID2, regionEvent)
	}
	{
		regionEvent := regionEvent{
			resolvedTs: 10,
			states:     []*regionFeedState{state3},
		}
		ds.Push(subID3, regionEvent)
	}

	// should only get one ts event
	{
		select {
		case <-tsCh:
			// the ts is from range lock, it is hard code in the test code
		case <-time.NewTimer(300 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}

		select {
		case <-tsCh:
			require.True(t, false, "shouldn't get an event")
		case <-time.NewTimer(300 * time.Millisecond).C:
		}
	}

	require.Equal(t, uint64(10), state1.getLastResolvedTs())
	require.Equal(t, uint64(11), state2.getLastResolvedTs())
	require.Equal(t, uint64(8), state3.getLastResolvedTs())
}

func TestHandleResolvedTsThrottled(t *testing.T) {
	ctx := context.Background()
	l := regionlock.NewRangeLock(1, []byte("a"), []byte("z"), math.MaxUint64)
	res1 := l.LockRange(ctx, []byte("a"), []byte("m"), 1, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res1.Status)
	res2 := l.LockRange(ctx, []byte("m"), []byte("z"), 2, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res2.Status)

	res1.LockedRangeState.Initialized.Store(true)
	res2.LockedRangeState.Initialized.Store(true)

	// Make the heap order deterministic, then update ResolvedTs without updating the heap to simulate a stale heap.
	res1.LockedRangeState.ResolvedTs.Store(1)
	l.UpdateLockedRangeStateHeap(res1.LockedRangeState)
	res2.LockedRangeState.ResolvedTs.Store(2)
	l.UpdateLockedRangeStateHeap(res2.LockedRangeState)
	require.Equal(t, uint64(1), l.GetHeapMinTs())

	res1.LockedRangeState.ResolvedTs.Store(300)
	res2.LockedRangeState.ResolvedTs.Store(200)
	require.Equal(t, uint64(200), l.ResolvedTs())
	require.Equal(t, uint64(300), l.GetHeapMinTs())

	span := &subscribedSpan{
		subID:           SubscriptionID(1),
		rangeLock:       l,
		advanceInterval: 100,
	}
	span.lastAdvanceTime.Store(0)
	state := newRegionFeedState(
		regionInfo{
			verID:            tikv.NewRegionVerID(1, 1, 1),
			subscribedSpan:   span,
			lockedRangeState: res1.LockedRangeState,
		},
		1,
		nil,
	)
	state.start()

	require.Equal(t, uint64(200), handleResolvedTs(span, state, 300))
}
