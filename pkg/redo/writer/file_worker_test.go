// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package writer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFlushAllDoesNotWaitAndReleasesCallbacksInFileOrder(t *testing.T) {
	firstFlushed := make(chan struct{})
	secondFlushed := make(chan struct{})
	var callbackOrder atomic.Int64
	var firstCallbackOrder atomic.Int64
	var secondCallbackOrder atomic.Int64

	first := &fileCache{
		flushed:   firstFlushed,
		postFlush: []func(){func() { firstCallbackOrder.Store(callbackOrder.Add(1)) }},
	}
	second := &fileCache{
		flushed:   secondFlushed,
		postFlush: []func(){func() { secondCallbackOrder.Store(callbackOrder.Add(1)) }},
	}
	worker := &fileWorkerGroup{
		files:      []*fileCache{first, second},
		flushCh:    make(chan *fileCache, 2),
		callbackCh: make(chan *fileCache, 2),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- worker.bgReleaseFileCallbacks(ctx)
	}()

	require.NoError(t, worker.flushAll(context.Background()))
	require.Same(t, first, <-worker.flushCh)
	require.Same(t, second, <-worker.flushCh)
	require.Empty(t, worker.files)

	close(secondFlushed)
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int64(0), callbackOrder.Load())
	close(firstFlushed)
	require.Eventually(t, func() bool {
		return callbackOrder.Load() == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(1), firstCallbackOrder.Load())
	require.Equal(t, int64(2), secondCallbackOrder.Load())

	cancel()
	require.ErrorIs(t, <-releaseDone, context.Canceled)
}
