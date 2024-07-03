package messaging

import (
	"encoding/binary"
	"fmt"

	. "github.com/flowbehappy/tigate/apperror"
	"github.com/google/uuid"
)

type IOType int32

const IOTypeSize int = 4
const (
	TypeInvalid IOType = 0

	TypeBytes    IOType = 1
	TypeServerId IOType = 2
	TypeDMLEvent IOType = 3
	TypeDDLEvent IOType = 4
)

var DefaultEndian = binary.LittleEndian

type Bytes []byte
type ServerId uuid.UUID

func (b *Bytes) encode(buf []byte) []byte    { return append(buf, (*b)...) }
func (s *ServerId) encode(buf []byte) []byte { return append(buf, (*s)[:]...) }

// Note that please never change the return slice directly,
// because the slice is a reference to the original data.
func (s *ServerId) slice() []byte { return (*s)[:] }

type DMLEvent struct {
	// TODO
}

func (d *DMLEvent) encode(buf []byte) []byte {
	// TODO
	return nil
}

type DDLEvent struct {
	// TODO
}

func (d *DDLEvent) encode(buf []byte) []byte {
	// TODO
	return nil
}

type IOTypeT interface {
	*Bytes | *ServerId | *DMLEvent | *DDLEvent

	encode(buf []byte) []byte
}

func CastTo[T IOTypeT](m interface{}) T {
	return m.(T)
}

type TargetMessage struct {
	From    ServerId
	To      ServerId
	Type    IOType
	Message interface{}
}

func encodeIOType[T IOTypeT](data *T, buf []byte) []byte {
	return (*data).encode(buf)
}

func decodeIOType(mtype IOType, data []byte) (interface{}, error) {
	switch mtype {
	case TypeBytes:
		return &data, nil
	case TypeServerId:
		if len(data) != 16 {
			return nil,
				AppError{Type: ErrorTypeImcomplete, Reason: fmt.Sprintf("data len is expected = %d, but got %d", 16, len(data))}
		}
		uid, err := uuid.ParseBytes(data)
		if err != nil {
			return nil, err
		}
		sid := ServerId(uid)
		return &sid, nil
	case TypeDMLEvent:
		// TODO
	case TypeDDLEvent:
		// TODO
	default:

	}
	return nil,
		AppError{Type: ErrorTypeInvalid, Reason: fmt.Sprintf("Invalid IOType: %d", mtype)}
}
