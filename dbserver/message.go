package dbserver

type MessageIdentifier uint8

const (
	Query MessageIdentifier = iota
	RowDescription
	DataRow
	CommandComplete
	ReadyForQuery
	AuthenticationOK
	ParameterStatus
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
