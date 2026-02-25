package dbserver

import "encoding/binary"

type MessageIdentifier rune

const (
	Query            MessageIdentifier = 'Q'
	RowDescription   MessageIdentifier = 'T'
	DataRow          MessageIdentifier = 'D'
	CommandComplete  MessageIdentifier = 'C'
	ReadyForQuery    MessageIdentifier = 'Z'
	AuthenticationOK MessageIdentifier = 'R'
	ParameterStatus  MessageIdentifier = 'S'
	ErrorResponse    MessageIdentifier = 'E'
)

const (
	SSLRequestMessageCode   = 80877103
	SSLRequestMessageLength = 8
)

// PostgreSQL OIDs for type identification in RowDescription.
const (
	OIDInt4 = 23
	OIDText = 25
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

// buildRowDescription builds a RowDescription ('T') message for the given fields.
// fieldTypes maps to PostgreSQL OIDs (OIDInt4 or OIDText).
func buildRowDescription(fields []string, fieldTypes []int) []byte {
	// Calculate payload size:
	// 2 bytes for field count
	// For each field: name(null-terminated) + 4(table OID) + 2(column attr) + 4(type OID) + 2(type size) + 4(type modifier) + 2(format code) = 18 bytes + len(name) + 1
	payloadSize := 2
	for _, f := range fields {
		payloadSize += len(f) + 1 + 18
	}

	buf := make([]byte, 0, payloadSize)
	// number of fields (Int16)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(fields)))

	for i, f := range fields {
		// field name (null-terminated string)
		buf = append(buf, []byte(f)...)
		buf = append(buf, 0)
		// table OID (Int32) - 0 for not a table column
		buf = binary.BigEndian.AppendUint32(buf, 0)
		// column attribute number (Int16) - 0
		buf = binary.BigEndian.AppendUint16(buf, 0)
		// data type OID (Int32)
		oid := uint32(OIDText)
		typeSize := int16(-1)   // variable length
		if fieldTypes[i] == 0 { // FieldTypeInt
			oid = OIDInt4
			typeSize = 4
		}
		buf = binary.BigEndian.AppendUint32(buf, oid)
		// data type size (Int16)
		buf = binary.BigEndian.AppendUint16(buf, uint16(typeSize))
		// type modifier (Int32) - -1
		buf = binary.BigEndian.AppendUint32(buf, 0xFFFFFFFF)
		// format code (Int16) - 0 = text
		buf = binary.BigEndian.AppendUint16(buf, 0)
	}

	msg := NewMessage(RowDescription, buf)
	return msg.ToByte()
}

// buildDataRow builds a DataRow ('D') message for a single row of string values.
func buildDataRow(values []string) []byte {
	// 2 bytes for column count + for each column: 4 bytes length + value bytes
	payloadSize := 2
	for _, v := range values {
		payloadSize += 4 + len(v)
	}

	buf := make([]byte, 0, payloadSize)
	// number of columns (Int16)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(values)))

	for _, v := range values {
		// column value length (Int32)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		// column value
		buf = append(buf, []byte(v)...)
	}

	msg := NewMessage(DataRow, buf)
	return msg.ToByte()
}

// buildCommandComplete builds a CommandComplete ('C') message with the given tag.
func buildCommandComplete(tag string) []byte {
	// tag is a null-terminated string
	payload := append([]byte(tag), 0)
	msg := NewMessage(CommandComplete, payload)
	return msg.ToByte()
}

// buildErrorResponse builds a simple ErrorResponse ('E') message.
func buildErrorResponse(severity, message string) []byte {
	var buf []byte
	// Severity field ('S')
	buf = append(buf, 'S')
	buf = append(buf, []byte(severity)...)
	buf = append(buf, 0)
	// Message field ('M')
	buf = append(buf, 'M')
	buf = append(buf, []byte(message)...)
	buf = append(buf, 0)
	// Terminator
	buf = append(buf, 0)
	msg := NewMessage(ErrorResponse, buf)
	return msg.ToByte()
}
