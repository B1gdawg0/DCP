package proto

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"sync"
)

var headerBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 96)
		return &b
	},
}

func EncodeHeader(w io.Writer, f *Frame) error {
	h := &f.Header
	h.AuthLen = uint16(len(f.Auth))
	h.PayloadLen = uint32(len(f.Payload))
	h.Checksum = crc32.ChecksumIEEE(f.Payload)

	ptr := headerBufPool.Get().(*[]byte)
	buf := (*ptr)[:0]
	defer func() {
		*ptr = buf
		headerBufPool.Put(ptr)
	}()

	buf = append(buf, h.Magic[:]...)
	buf = append(buf, h.Version)
	buf = append(buf, byte(h.Type))
	buf = append(buf, h.MessageID[:]...)
	buf = append(buf, h.RequestID[:]...)
	buf = append(buf, h.CorrelationID[:]...)
	buf = append(buf, h.SenderID[:]...)
	buf = appendUint64(buf, uint64(h.Timestamp))
	buf = appendUint64(buf, uint64(h.Deadline))
	buf = appendUint32(buf, h.ServiceID)
	buf = appendUint32(buf, h.OpID)
	buf = appendUint16(buf, h.Flags)
	buf = appendUint16(buf, h.AuthLen)
	buf = appendUint32(buf, h.PayloadLen)
	buf = appendUint32(buf, h.Checksum)

	_, err := w.Write(buf)
	return err
}

func appendUint16(b []byte, v uint16) []byte {
	return append(b, byte(v>>8), byte(v))
}

func appendUint32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendUint64(b []byte, v uint64) []byte {
	return append(b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v),
	)
}

func EncodeFrame(f *Frame) ([]byte, error) {
	h := &f.Header
	h.AuthLen = uint16(len(f.Auth))
	h.PayloadLen = uint32(len(f.Payload))
	h.Checksum = crc32.ChecksumIEEE(f.Payload)

	ptr := headerBufPool.Get().(*[]byte)
	buf := (*ptr)[:0]
	defer func() {
		*ptr = buf
		headerBufPool.Put(ptr)
	}()

	buf = append(buf, h.Magic[:]...)
	buf = append(buf, h.Version)
	buf = append(buf, byte(h.Type))
	buf = append(buf, h.MessageID[:]...)
	buf = append(buf, h.RequestID[:]...)
	buf = append(buf, h.CorrelationID[:]...)
	buf = append(buf, h.SenderID[:]...)
	buf = appendUint64(buf, uint64(h.Timestamp))
	buf = appendUint64(buf, uint64(h.Deadline))
	buf = appendUint32(buf, h.ServiceID)
	buf = appendUint32(buf, h.OpID)
	buf = appendUint16(buf, h.Flags)
	buf = appendUint16(buf, h.AuthLen)
	buf = appendUint32(buf, h.PayloadLen)
	buf = appendUint32(buf, h.Checksum)

	total := len(buf) + len(f.Auth) + len(f.Payload)
	result := make([]byte, 0, total)
	result = append(result, buf...)
	result = append(result, f.Auth...)
	result = append(result, f.Payload...)
	return result, nil
}

func encodeBinary(w io.Writer, v any) error {
	return binary.Write(w, binary.BigEndian, v)
}