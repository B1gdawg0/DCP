package proto

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

func DecodeFrame(r io.Reader) (*Frame, error) {
	var h Header

	const maxAuth = 65535
	const maxPayload = 10 * 1024 * 1024

	// --- HEADER ---
	if _, err := io.ReadFull(r, h.Magic[:]); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.BigEndian, &h.Version); err != nil {
		return nil, err
	}

	var t uint8
	if err := binary.Read(r, binary.BigEndian, &t); err != nil {
		return nil, err
	}
	h.Type = MessageType(t)

	if _, err := io.ReadFull(r, h.MessageID[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r, h.RequestID[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r, h.CorrelationID[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r, h.SenderID[:]); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.BigEndian, &h.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &h.Deadline); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &h.ServiceID); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &h.OpID); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &h.Flags); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &h.AuthLen); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &h.PayloadLen); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &h.Checksum); err != nil {
		return nil, err
	}

	//validation
	if h.Magic != [2]byte{0xDC, 0x50} {
		return nil, errors.New("invalid magic")
	}

	if h.AuthLen > maxAuth {
		return nil, errors.New("auth too large")
	}

	if h.PayloadLen > maxPayload {
		return nil, errors.New("payload too large")
	}

	//body
	auth := make([]byte, h.AuthLen)
	if _, err := io.ReadFull(r, auth); err != nil {
		return nil, err
	}

	payload := make([]byte, h.PayloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	//checksum validation
	if crc32.ChecksumIEEE(payload) != h.Checksum {
		return nil, errors.New("checksum mismatch")
	}

	return &Frame{
		Header:  h,
		Auth:    auth,
		Payload: payload,
	}, nil
}