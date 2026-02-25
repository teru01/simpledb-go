package dbserver

import "encoding/binary"

type MessageIdentifier rune

const (
	Query            MessageIdentifier = 'Q'
	RowDescription                     = 'T'
	DataRow                            = 'D'
	CommandComplete                    = 'C'
	ReadyForQuery                      = 'Z'
	AuthenticationOK                   = 'R'
	ParameterStatus                    = 'S'
	ErrorResponse                      = 'E'
)

const (
	SSLRequestMessageCode   = 80877103
	SSLRequestMessageLength = 8
)

type Message struct {
	identifier MessageIdentifier
	length     int32
	payload    []byte
}

func NewMessage(identifier MessageIdentifier, payload []byte) Message {
	return Message{
		identifier: identifier,
		length:     int32(len(payload) + 4), // including length field
		payload:    payload,
	}
}

func (m *Message) ToByte() []byte {
	buf := make([]byte, 5+len(m.payload))
	buf[0] = byte(m.identifier)
	binary.BigEndian.PutUint32(buf[1:5], uint32(m.length))
	copy(buf[5:], m.payload)
	return buf
}
