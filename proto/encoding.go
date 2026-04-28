package proto

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

func EncodeFrame(f *Frame) ([]byte, error) {
	h := &f.Header

	h.AuthLen = uint16(len(f.Auth))
	h.PayloadLen = uint32(len(f.Payload))
	h.Checksum = crc32.ChecksumIEEE(f.Payload)

	buf := new(bytes.Buffer)

	// --- HEADER ---
	buf.Write(h.Magic[:])
	buf.WriteByte(h.Version)
	buf.WriteByte(byte(h.Type))

	buf.Write(h.MessageID[:])
	buf.Write(h.RequestID[:])
	buf.Write(h.CorrelationID[:])
	buf.Write(h.SenderID[:])

	binary.Write(buf, binary.BigEndian, h.Timestamp)
	binary.Write(buf, binary.BigEndian, h.Deadline)
	binary.Write(buf, binary.BigEndian, h.ServiceID)
	binary.Write(buf, binary.BigEndian, h.OpID)
	binary.Write(buf, binary.BigEndian, h.Flags)
	binary.Write(buf, binary.BigEndian, h.AuthLen)
	binary.Write(buf, binary.BigEndian, h.PayloadLen)
	binary.Write(buf, binary.BigEndian, h.Checksum)

	// --- BODY ---
	if h.AuthLen > 0 {
		buf.Write(f.Auth)
	}
	if h.PayloadLen > 0 {
		buf.Write(f.Payload)
	}
	// if h.Flags&FlagCompressed != 0 {
	// 	f.Payload = compress(f.Payload)
	// }

	return buf.Bytes(), nil
}