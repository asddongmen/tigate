package messaging

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/pkg/node"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/ticdc/pkg/metrics"

	"github.com/pingcap/log"
	. "github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/utils/conn"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type MessageTarget interface {
	Epoch() uint64
}

const (
	reconnectInterval = 2 * time.Second
	msgTypeEvent      = "event"
	msgTypeCommand    = "command"
)

// remoteMessageTarget implements the SendMessageChannel interface.
// TODO: Reduce the goroutine number it spawns.
// Currently it spawns 2 goroutines for each remote target, and 2 goroutines for each local target,
// and 1 goroutine to handle grpc stream error.
type remoteMessageTarget struct {
	messageCenterID    node.ID
	messageCenterEpoch uint64

	targetEpoch atomic.Value
	targetId    node.ID
	targetAddr  string

	// For sending events and commands
	eventSender   *sendStreamWrapper
	commandSender *sendStreamWrapper

	// For receiving events and commands
	conn              *grpc.ClientConn
	eventRecvStream   grpcReceiver
	commandRecvStream grpcReceiver

	// We push the events and commands to remote send streams.
	// The send streams are created when the target is added to the message center.
	// These channels are used to cache the messages before sending.
	sendEventCh chan *proto.Message
	sendCmdCh   chan *proto.Message

	// We pull the events and commands from remote receive streams,
	// and push to the message center.
	recvEventCh chan *TargetMessage
	recvCmdCh   chan *TargetMessage

	wg *sync.WaitGroup
	// ctx is used to create the grpc stream.
	ctx context.Context
	// cancel is used to stop the grpc stream, and the goroutine spawned by remoteMessageTarget.
	cancel context.CancelFunc
	// errCh is used to gather the error from the goroutine spawned by remoteMessageTarget.
	errCh chan AppError

	sendEventCounter           prometheus.Counter
	dropEventCounter           prometheus.Counter
	recvEventCounter           prometheus.Counter
	congestedEventErrorCounter prometheus.Counter

	sendCmdCounter           prometheus.Counter
	dropCmdCounter           prometheus.Counter
	recvCmdCounter           prometheus.Counter
	congestedCmdErrorCounter prometheus.Counter

	receivedFailedErrorCounter     prometheus.Counter
	connectionNotfoundErrorCounter prometheus.Counter
	connectionFailedErrorCounter   prometheus.Counter
}

func (s *remoteMessageTarget) isReadyToSend() bool {
	return s.eventSender.ready.Load() && s.commandSender.ready.Load()
}

func (s *remoteMessageTarget) Epoch() uint64 {
	return s.targetEpoch.Load().(uint64)
}

func (s *remoteMessageTarget) sendEvent(msg ...*TargetMessage) error {
	if !s.eventSender.ready.Load() {
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Stream has not been initialized"}
	}
	select {
	case <-s.ctx.Done():
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Stream has been closed"}
	case s.sendEventCh <- s.newMessage(msg...):
		s.sendEventCounter.Add(float64(len(msg)))
		return nil
	default:
		s.congestedEventErrorCounter.Inc()
		return AppError{Type: ErrorTypeMessageCongested, Reason: "Send event message is congested"}
	}
}

func (s *remoteMessageTarget) sendCommand(msg ...*TargetMessage) error {
	if !s.commandSender.ready.Load() {
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Stream has not been initialized"}
	}
	select {
	case <-s.ctx.Done():
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: "Stream has been closed"}
	case s.sendCmdCh <- s.newMessage(msg...):
		s.sendCmdCounter.Add(float64(len(msg)))
		return nil
	default:
		s.congestedCmdErrorCounter.Inc()
		return AppError{Type: ErrorTypeMessageCongested, Reason: "Send command message is congested"}
	}
}

func newRemoteMessageTarget(
	localID, targetId node.ID,
	localEpoch, targetEpoch uint64,
	addr string,
	recvEventCh, recvCmdCh chan *TargetMessage,
	cfg *config.MessageCenterConfig,
) *remoteMessageTarget {
	log.Info("Create remote target", zap.Stringer("local", localID), zap.Stringer("remote", targetId), zap.Any("addr", addr), zap.Any("localEpoch", localEpoch), zap.Any("targetEpoch", targetEpoch))
	ctx, cancel := context.WithCancel(context.Background())
	rt := &remoteMessageTarget{
		messageCenterID:    localID,
		messageCenterEpoch: localEpoch,
		targetAddr:         addr,
		targetId:           targetId,
		eventSender:        &sendStreamWrapper{ready: atomic.Bool{}},
		commandSender:      &sendStreamWrapper{ready: atomic.Bool{}},
		ctx:                ctx,
		cancel:             cancel,
		sendEventCh:        make(chan *proto.Message, cfg.CacheChannelSize),
		sendCmdCh:          make(chan *proto.Message, cfg.CacheChannelSize),
		recvEventCh:        recvEventCh,
		recvCmdCh:          recvCmdCh,
		errCh:              make(chan AppError, 8),
		wg:                 &sync.WaitGroup{},

		sendEventCounter:           metrics.MessagingSendMsgCounter.WithLabelValues(string(addr), "event"),
		dropEventCounter:           metrics.MessagingDropMsgCounter.WithLabelValues(string(addr), "event"),
		recvEventCounter:           metrics.MessagingReceiveMsgCounter.WithLabelValues(string(addr), "event"),
		congestedEventErrorCounter: metrics.MessagingErrorCounter.WithLabelValues(string(addr), "event", "message_congested"),

		sendCmdCounter:           metrics.MessagingSendMsgCounter.WithLabelValues(string(addr), "command"),
		dropCmdCounter:           metrics.MessagingDropMsgCounter.WithLabelValues(string(addr), "command"),
		recvCmdCounter:           metrics.MessagingReceiveMsgCounter.WithLabelValues(string(addr), "command"),
		congestedCmdErrorCounter: metrics.MessagingErrorCounter.WithLabelValues(string(addr), "command", "message_congested"),

		receivedFailedErrorCounter:     metrics.MessagingErrorCounter.WithLabelValues(string(addr), "message", "message_received_failed"),
		connectionNotfoundErrorCounter: metrics.MessagingErrorCounter.WithLabelValues(string(addr), "message", "connection_not_found"),
		connectionFailedErrorCounter:   metrics.MessagingErrorCounter.WithLabelValues(string(addr), "message", "connection_failed"),
	}
	rt.targetEpoch.Store(targetEpoch)
	rt.runHandleErr(ctx)
	return rt
}

// close stops the grpc stream and the goroutine spawned by remoteMessageTarget.
func (s *remoteMessageTarget) close() {
	log.Info("Close remote target", zap.Any("messageCenterID", s.messageCenterID), zap.Any("remote", s.targetId), zap.Any("addr", s.targetAddr))
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.cancel()
	s.wg.Wait()
}

func (s *remoteMessageTarget) runHandleErr(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-s.errCh:
				switch err.Type {
				case ErrorTypeMessageReceiveFailed, ErrorTypeConnectionFailed:
					log.Warn("received message from remote failed, will be reconnect",
						zap.Any("messageCenterID", s.messageCenterID), zap.Any("remote", s.targetId), zap.Error(err))
					time.Sleep(reconnectInterval)
					s.resetConnect()
				default:
					log.Error("Error in remoteMessageTarget, error:", zap.Error(err))
				}

			}
		}
	}()
}

func (s *remoteMessageTarget) collectErr(err AppError) {
	switch err.Type {
	case ErrorTypeMessageReceiveFailed:
		s.receivedFailedErrorCounter.Inc()
	case ErrorTypeConnectionFailed:
		s.connectionFailedErrorCounter.Inc()
	}
	select {
	case s.errCh <- err:
	default:
	}
}

func (s *remoteMessageTarget) connect() {
	if s.conn != nil {
		return
	}
	conn, err := conn.Connect(string(s.targetAddr), &security.Credential{})
	if err != nil {
		log.Info("Cannot create grpc client",
			zap.Any("messageCenterID", s.messageCenterID), zap.Any("remote", s.targetId), zap.Error(err))
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot create grpc client on address %s, error: %s", s.targetAddr, err.Error())})
		return
	}

	client := proto.NewMessageCenterClient(conn)
	handshake := &proto.Message{
		From:  string(s.messageCenterID),
		To:    string(s.targetId),
		Epoch: uint64(s.messageCenterEpoch),
		Type:  int32(TypeMessageHandShake),
	}

	eventStream, err := client.SendEvents(s.ctx, handshake)
	if err != nil {
		log.Info("Cannot establish event grpc stream",
			zap.Any("messageCenterID", s.messageCenterID), zap.Stringer("remote", s.targetId), zap.Error(err))
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error())})
		return
	}

	commandStream, err := client.SendCommands(s.ctx, handshake)
	if err != nil {
		log.Info("Cannot establish command grpc stream",
			zap.Any("messageCenterID", s.messageCenterID), zap.Stringer("remote", s.targetId), zap.Error(err))
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error())})
		return
	}

	s.conn = conn
	s.eventRecvStream = eventStream
	s.commandRecvStream = commandStream
	s.runReceiveMessages(eventStream, s.recvEventCh)
	s.runReceiveMessages(commandStream, s.recvCmdCh)
	log.Info("Connected to remote target",
		zap.Any("messageCenterID", s.messageCenterID),
		zap.Any("remote", s.targetId),
		zap.Any("remoteAddr", s.targetAddr))
}

func (s *remoteMessageTarget) resetConnect() {
	log.Info("reconnect to remote target",
		zap.Any("messageCenterID", s.messageCenterID),
		zap.Any("remote", s.targetId))
	// Close the old streams
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}

	s.eventRecvStream = nil
	s.commandRecvStream = nil
	// Clear the error channel
LOOP:
	for {
		select {
		case <-s.errCh:
		default:
			break LOOP
		}
	}
	// Reconnect
	s.connect()
}

func (s *remoteMessageTarget) runEventSendStream(eventStream grpcSender) error {
	s.eventSender.stream = eventStream
	s.eventSender.ready.Store(true)
	err := s.runSendMessages(s.ctx, s.eventSender.stream, s.sendEventCh)
	log.Info("Event send stream closed",
		zap.Any("messageCenterID", s.messageCenterID), zap.Any("remote", s.targetId), zap.Error(err))
	s.eventSender.ready.Store(false)
	return err
}

func (s *remoteMessageTarget) runCommandSendStream(commandStream grpcSender) error {
	s.commandSender.stream = commandStream
	s.commandSender.ready.Store(true)
	err := s.runSendMessages(s.ctx, s.commandSender.stream, s.sendCmdCh)
	log.Info("Command send stream closed",
		zap.Any("messageCenterID", s.messageCenterID), zap.Any("remote", s.targetId), zap.Error(err))
	s.commandSender.ready.Store(false)
	return err
}

func (s *remoteMessageTarget) runSendMessages(sendCtx context.Context, stream grpcSender, sendChan chan *proto.Message) error {
	for {
		select {
		case <-sendCtx.Done():
			return sendCtx.Err()
		case message := <-sendChan:
			if err := stream.Send(message); err != nil {
				log.Error("Error when sending message to remote",
					zap.Error(err),
					zap.Any("messageCenterID", s.messageCenterID),
					zap.Any("remote", s.targetId))
				err = AppError{Type: ErrorTypeMessageSendFailed, Reason: err.Error()}
				return err
			}
		}
	}
}

func (s *remoteMessageTarget) runReceiveMessages(stream grpcReceiver, receiveCh chan *TargetMessage) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			message, err := stream.Recv()
			if err != nil {
				err := AppError{Type: ErrorTypeMessageReceiveFailed, Reason: errors.Trace(err).Error()}
				// return the error to close the stream, the client side is responsible to reconnect.
				s.collectErr(err)
				return
			}
			mt := IOType(message.Type)
			if mt == TypeMessageHandShake {
				log.Info("Received handshake message", zap.Any("messageCenterID", s.messageCenterID), zap.Any("remote", s.targetId))
				continue
			}
			targetMsg := &TargetMessage{
				From:     node.ID(message.From),
				To:       node.ID(message.To),
				Topic:    message.Topic,
				Epoch:    message.Epoch,
				Sequence: message.Seqnum,
				Type:     mt,
			}
			for _, payload := range message.Payload {
				msg, err := decodeIOType(mt, payload)
				if err != nil {
					// TODO: handle this error properly.
					err := AppError{Type: ErrorTypeInvalidMessage, Reason: errors.Trace(err).Error()}
					log.Panic("Failed to decode message", zap.Error(err))
				}
				targetMsg.Message = append(targetMsg.Message, msg)
			}
			receiveCh <- targetMsg
		}
	}()
}

func (s *remoteMessageTarget) newMessage(msg ...*TargetMessage) *proto.Message {
	msgBytes := make([][]byte, 0, len(msg))
	for _, tm := range msg {
		for _, im := range tm.Message {
			// TODO: use a buffer pool to reduce the memory allocation.
			buf, err := im.Marshal()
			if err != nil {
				log.Panic("marshal message failed ",
					zap.Any("msg", im),
					zap.Error(err))
			}
			msgBytes = append(msgBytes, buf)
		}
	}
	protoMsg := &proto.Message{
		From:    string(s.messageCenterID),
		To:      string(s.targetId),
		Epoch:   uint64(s.messageCenterEpoch),
		Topic:   string(msg[0].Topic),
		Type:    int32(msg[0].Type),
		Payload: msgBytes,
	}
	return protoMsg
}

// localMessageTarget implements the SendMessageChannel interface.
// It is used to send messages to the local server.
// It simply pushes the messages to the messageCenter's channel directly.
type localMessageTarget struct {
	localId  node.ID
	epoch    uint64
	sequence atomic.Uint64

	// The gather channel from the message center.
	// We only need to push and pull the messages from those channel.
	recvEventCh chan *TargetMessage
	recvCmdCh   chan *TargetMessage

	sendEventCounter   prometheus.Counter
	dropMessageCounter prometheus.Counter
	sendCmdCounter     prometheus.Counter
}

func (s *localMessageTarget) Epoch() uint64 {
	return s.epoch
}

func (s *localMessageTarget) sendEvent(msg *TargetMessage) error {
	err := s.sendMsgToChan(s.recvEventCh, true, msg)
	if err != nil {
		s.recordCongestedMessageError(msgTypeEvent)
	} else {
		s.sendEventCounter.Inc()
	}
	return err
}

func (s *localMessageTarget) sendCommand(msg *TargetMessage) error {
	err := s.sendMsgToChan(s.recvCmdCh, false, msg)
	if err != nil {
		s.recordCongestedMessageError(msgTypeCommand)
	} else {
		s.sendCmdCounter.Inc()
	}
	return err
}

func newLocalMessageTarget(id node.ID,
	gatherRecvEventChan chan *TargetMessage,
	gatherRecvCmdChan chan *TargetMessage) *localMessageTarget {
	return &localMessageTarget{
		localId:            id,
		recvEventCh:        gatherRecvEventChan,
		recvCmdCh:          gatherRecvCmdChan,
		sendEventCounter:   metrics.MessagingSendMsgCounter.WithLabelValues("local", "event"),
		dropMessageCounter: metrics.MessagingDropMsgCounter.WithLabelValues("local", "message"),
		sendCmdCounter:     metrics.MessagingSendMsgCounter.WithLabelValues("local", "command"),
	}
}

func (s *localMessageTarget) recordCongestedMessageError(typeE string) {
	metrics.MessagingErrorCounter.WithLabelValues("local", typeE, "message_congested").Inc()
}

func (s *localMessageTarget) sendMsgToChan(ch chan *TargetMessage, force bool, msg ...*TargetMessage) error {
	for _, m := range msg {
		m.To = s.localId
		m.From = s.localId
		m.Epoch = s.epoch
		m.Sequence = s.sequence.Add(1)
		if force {
			ch <- m
		} else {
			select {
			case ch <- m:
			default:
				return AppError{Type: ErrorTypeMessageCongested, Reason: "Send message is congested"}
			}
		}
	}
	return nil
}

type sendStreamWrapper struct {
	stream grpcSender
	ready  atomic.Bool
}
