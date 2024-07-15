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

package downstreamadapter

import (
	"github.com/flowbehappy/tigate/downstreamadapter/dispatchermanager"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const MaintainerBoostrapRequestTopic = "maintainerBoostrapRequest"
const MaintainerBoostrapResponseTopic = "maintainerBoostrapResponse"

type DispatcherManagerManager struct {
	dispatcherManagers map[model.ChangeFeedID]*dispatchermanager.EventDispatcherManager
}

func NewDispatcherManagerManager() *DispatcherManagerManager {
	m := &DispatcherManagerManager{
		dispatcherManagers: make(map[model.ChangeFeedID]*dispatchermanager.EventDispatcherManager),
	}
	context.GetMessageCenter().RegisterHandler(MaintainerBoostrapRequestTopic, m.RecvMaintainerBootstrapRequest)
	return m
}

func (m *DispatcherManagerManager) RecvMaintainerBootstrapRequest(msg *messaging.TargetMessage) error {
	maintainerBootstrapRequest := msg.Message.(*heartbeatpb.MaintainerBootstrapRequest)
	changefeedID := model.DefaultChangeFeedID(maintainerBootstrapRequest.ChangefeedID)

	eventDispatcherManager, ok := m.dispatcherManagers[changefeedID]
	if !ok {
		eventDispatcherManager := dispatchermanager.NewEventDispatcherManager(changefeedID, xxx, config, msg.To, msg.From)
		m.dispatcherManagers[changefeedID] = eventDispatcherManager

		response := &heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: maintainerBootstrapRequest.ChangefeedID,
			Statuses:     make([]*heartbeatpb.TableSpanStatus, 0),
		}

		err := context.GetMessageCenter().SendCommand(messaging.NewTargetMessage(
			msg.From,
			MaintainerBoostrapResponseTopic,
			response,
		))
		if err != nil {
			log.Error("failed to send maintainer bootstrap response", zap.Error(err))
			return err
		}
		return nil
	}

	response := &heartbeatpb.MaintainerBootstrapResponse{
		Statuses: make([]*heartbeatpb.TableSpanStatus, 0, len(eventDispatcherManager.DispatcherMap)),
	}
	for _, dispatcher := range eventDispatcherManager.DispatcherMap {
		response.Statuses = append(response.Statuses, &heartbeatpb.TableSpanStatus{
			Span: &heartbeatpb.TableSpan{
				TableID:  dispatcher.GetTableSpan().GetTableID(),
				StartKey: dispatcher.GetTableSpan().GetStartKey(),
				EndKey:   dispatcher.GetTableSpan().GetEndKey(),
			},
			ComponentStatus: int32(dispatcher.GetComponentStatus()),
			CheckpointTs:    0,
		})
	}
	err := context.GetMessageCenter().SendCommand(messaging.NewTargetMessage(
		msg.From,
		MaintainerBoostrapResponseTopic,
		response,
	))
	if err != nil {
		log.Error("failed to send maintainer bootstrap response", zap.Error(err))
		return err
	}
	return nil
}