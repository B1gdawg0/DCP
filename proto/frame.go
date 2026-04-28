package proto

// Ref: Spec-1.0
type MessageType uint8

const (
    TypeRequest   MessageType = 1
    TypeQuery     MessageType = 2
    TypeAccepted  MessageType = 3
    TypeRejected  MessageType = 4
    TypeCompleted MessageType = 5
    TypeFailed    MessageType = 6
    TypeCancelled MessageType = 7
    TypeExpired   MessageType = 8
    TypeACK       MessageType = 9
)

type Frame struct {
    Header  Header
    Auth    []byte
    Payload []byte
}

type Header struct {
    Magic     [2]byte     // 0xDC 0x50 ("DCP")
    Version   uint8
    Type      MessageType
    MessageID [16]byte    // UUID
    RequestID [16]byte
	CorrelationID [16]byte
    SenderID  [8]byte
    Timestamp int64       // unix nanoseconds
    Deadline  int64       // unix nanoseconds
    ServiceID uint32
    OpID      uint32
    Flags     uint16
    AuthLen   uint16
    PayloadLen uint32

	Checksum uint32
}

// const (
//     FlagCompressed uint16 = 1 << 0
// )