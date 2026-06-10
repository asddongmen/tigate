// Copyright 2026 PingCAP, Inc.
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

package kafka

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type dryRunFactory struct {
	changefeedID commonType.ChangeFeedID
	option       *options
}

func newDryRunFactory(o *options, changefeedID commonType.ChangeFeedID) Factory {
	return &dryRunFactory{
		changefeedID: changefeedID,
		option:       o,
	}
}

func (f *dryRunFactory) AdminClient(context.Context) (ClusterAdminClient, error) {
	return &dryRunAdminClient{
		changefeedID:      f.changefeedID,
		topic:             f.option.Topic,
		partitionNum:      f.option.PartitionNum,
		replicationFactor: f.option.ReplicationFactor,
	}, nil
}

func (f *dryRunFactory) SyncProducer(context.Context) (SyncProducer, error) {
	return &dryRunSyncProducer{
		changefeedID: f.changefeedID,
		delay:        f.option.DryRunDelay,
		closed:       atomic.NewBool(false),
	}, nil
}

func (f *dryRunFactory) AsyncProducer(context.Context) (AsyncProducer, error) {
	return &dryRunAsyncProducer{
		changefeedID: f.changefeedID,
		delay:        f.option.DryRunDelay,
		closed:       atomic.NewBool(false),
	}, nil
}

func (f *dryRunFactory) MetricsCollector(ClusterAdminClient) MetricsCollector {
	return &dryRunMetricsCollector{}
}

type dryRunAdminClient struct {
	changefeedID      commonType.ChangeFeedID
	topic             string
	partitionNum      int32
	replicationFactor int16
	closed            bool
}

func (a *dryRunAdminClient) GetAllBrokers() []Broker {
	if a.closed {
		return nil
	}
	return []Broker{{ID: 0}}
}

func (a *dryRunAdminClient) GetBrokerConfig(configName string) (string, error) {
	switch configName {
	case BrokerMessageMaxBytesConfigName, TopicMaxMessageBytesConfigName:
		return "1073741824", nil
	case MinInsyncReplicasConfigName:
		return "1", nil
	case BrokerConnectionsMaxIdleMsConfigName:
		return "300000", nil
	default:
		return "", cerror.ErrKafkaConfigNotFound.GenWithStack(
			"cannot find the `%s` from the dry-run Kafka broker configuration", configName)
	}
}

func (a *dryRunAdminClient) GetTopicConfig(_ string, configName string) (string, error) {
	return a.GetBrokerConfig(configName)
}

func (a *dryRunAdminClient) GetTopicsMeta(topics []string, _ bool) (map[string]TopicDetail, error) {
	result := make(map[string]TopicDetail, len(topics))
	for _, topic := range topics {
		result[topic] = TopicDetail{
			Name:              topic,
			NumPartitions:     a.partitionNum,
			ReplicationFactor: a.replicationFactor,
		}
	}
	return result, nil
}

func (a *dryRunAdminClient) GetTopicsPartitionsNum(topics []string) (map[string]int32, error) {
	result := make(map[string]int32, len(topics))
	for _, topic := range topics {
		result[topic] = a.partitionNum
	}
	return result, nil
}

func (a *dryRunAdminClient) CreateTopic(detail *TopicDetail, _ bool) error {
	if detail == nil {
		return nil
	}
	a.topic = detail.Name
	a.partitionNum = detail.NumPartitions
	a.replicationFactor = detail.ReplicationFactor
	return nil
}

func (a *dryRunAdminClient) Heartbeat() {}

func (a *dryRunAdminClient) Close() {
	a.closed = true
}

type dryRunSyncProducer struct {
	changefeedID commonType.ChangeFeedID
	delay        time.Duration
	closed       *atomic.Bool
}

func (p *dryRunSyncProducer) SendMessage(_ string, _ int32, _ *common.Message) error {
	return p.send()
}

func (p *dryRunSyncProducer) SendMessages(_ string, _ int32, _ *common.Message) error {
	return p.send()
}

func (p *dryRunSyncProducer) send() error {
	if p.closed.Load() {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}
	time.Sleep(p.delay)
	return nil
}

func (p *dryRunSyncProducer) Heartbeat() {}

func (p *dryRunSyncProducer) Close() {
	if p.closed.Swap(true) {
		return
	}
	log.Info("Kafka dry-run DDL producer closed",
		zap.String("keyspace", p.changefeedID.Keyspace()),
		zap.String("changefeed", p.changefeedID.Name()))
}

type dryRunAsyncProducer struct {
	changefeedID commonType.ChangeFeedID
	delay        time.Duration
	closed       *atomic.Bool
}

func (p *dryRunAsyncProducer) Close() {
	if p.closed.Swap(true) {
		return
	}
	log.Info("Kafka dry-run DML producer closed",
		zap.String("keyspace", p.changefeedID.Keyspace()),
		zap.String("changefeed", p.changefeedID.Name()))
}

func (p *dryRunAsyncProducer) AsyncSend(ctx context.Context, _ string, _ int32, message *common.Message) error {
	if p.closed.Load() {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}
	if p.delay > 0 {
		timer := time.NewTimer(p.delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Trace(ctx.Err())
		case <-timer.C:
		}
	}
	if message != nil && message.Callback != nil {
		message.Callback()
	}
	return nil
}

func (p *dryRunAsyncProducer) Heartbeat() {}

func (p *dryRunAsyncProducer) AsyncRunCallback(ctx context.Context) error {
	<-ctx.Done()
	return errors.Trace(ctx.Err())
}

type dryRunMetricsCollector struct{}

func (m *dryRunMetricsCollector) Run(context.Context) {}
