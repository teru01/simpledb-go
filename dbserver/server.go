package dbserver

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"

	"github.com/teru01/simpledb-go/dbexecutor"
)

func StartServer(ctx context.Context, addr string, db *dbexecutor.SimpleDB) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	slog.Info("server listening", "addr", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("accept error", "error", err)
			continue
		}
		go func() {
			if err := handleConn(ctx, conn, db); err != nil {
				slog.Error("connection error", "error", err)
			}
		}()
	}
}

func handleConn(ctx context.Context, conn net.Conn, db *dbexecutor.SimpleDB) error {
	defer conn.Close()
	slog.Info("new connection", "remote", conn.RemoteAddr())
	if err := handleStartup(conn); err != nil {
		return fmt.Errorf("handle startup: %w", err)
	}
	if err := handleQueryLoop(ctx, conn, db); err != nil {
		return fmt.Errorf("handle query: %w", err)
	}
	slog.Info("connection closed", "remote", conn.RemoteAddr())
	return nil
}

func handleStartup(conn net.Conn) error {
	for {
		var messageLen int32
		if err := binary.Read(conn, binary.BigEndian, &messageLen); err != nil {
			return fmt.Errorf("read length: %w", err)
		}

		if messageLen == SSLRequestMessageLength {
			var sslRequestCode int32
			if err := binary.Read(conn, binary.BigEndian, &sslRequestCode); err != nil {
				return fmt.Errorf("read sslRequest code: %w", err)
			}
			if sslRequestCode == SSLRequestMessageCode {
				// Deny SSL, client will retry with StartupMessage
				if _, err := conn.Write([]byte("N")); err != nil {
					return fmt.Errorf("respond N for ssl request: %w", err)
				}
				continue // read the next message (StartupMessage)
			}
		}

		// StartupMessage
		payload := make([]byte, messageLen-4)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return fmt.Errorf("read startup payload: %w", err)
		}

		// Send AuthenticationOk
		ok := NewMessage(AuthenticationOK, []byte{0, 0, 0, 0})
		if _, err := conn.Write(ok.ToByte()); err != nil {
			return fmt.Errorf("write authentication ok: %w", err)
		}

		// Send ReadyForQuery (idle)
		ready := NewMessage(ReadyForQuery, []byte("I"))
		if _, err := conn.Write(ready.ToByte()); err != nil {
			return fmt.Errorf("write ready for query: %w", err)
		}
		return nil
	}
}

func handleQueryLoop(ctx context.Context, conn net.Conn, db *dbexecutor.SimpleDB) error {
	for {
		sql, terminate, err := readQuery(conn)
		if err != nil {
			return fmt.Errorf("read query: %w", err)
		}
		if terminate {
			return nil
		}
		if sql == "" {
			continue
		}

		slog.Debug("executing query", "sql", sql)
		result, err := db.Execute(ctx, sql)
		if err != nil {
			slog.Error("query execution error", "sql", sql, "error", err)
			if _, wErr := conn.Write(buildErrorResponse("ERROR", err.Error())); wErr != nil {
				return fmt.Errorf("write error response: %w", wErr)
			}
			ready := NewMessage(ReadyForQuery, []byte("I"))
			if _, wErr := conn.Write(ready.ToByte()); wErr != nil {
				return fmt.Errorf("write ready for query: %w", wErr)
			}
			continue
		}

		if err := writeResult(conn, result); err != nil {
			return fmt.Errorf("write result: %w", err)
		}
	}
}

// readQuery reads a single frontend message. Returns the SQL string for Query messages,
// and terminate=true for Terminate messages.
func readQuery(conn io.ReadWriter) (sql string, terminate bool, err error) {
	identifierBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, identifierBuf); err != nil {
		return "", false, fmt.Errorf("read identifier: %w", err)
	}
	identifier := MessageIdentifier(rune(identifierBuf[0]))

	var payloadLen int32
	if err := binary.Read(conn, binary.BigEndian, &payloadLen); err != nil {
		return "", false, fmt.Errorf("read payload length: %w", err)
	}

	payload := make([]byte, payloadLen-4)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return "", false, fmt.Errorf("read payload: %w", err)
	}

	switch identifier {
	case 'X': // Terminate
		return "", true, nil
	case Query:
		// Query payload is a null-terminated string
		s := strings.TrimRight(string(payload), "\x00")
		return strings.TrimSpace(s), false, nil
	default:
		slog.Debug("unexpected identifier", "identifier", string(identifierBuf))
		if _, err := conn.Write(buildErrorResponse("ERROR", "unsupported message type")); err != nil {
			return "", false, fmt.Errorf("write error response: %w", err)
		}
		return "", false, nil
	}
}

// writeResult sends the query result back to the client using the PostgreSQL wire protocol.
func writeResult(conn net.Conn, result *dbexecutor.ExecuteResult) error {
	// For SELECT queries, send RowDescription + DataRows
	if len(result.Fields) > 0 {
		if _, err := conn.Write(buildRowDescription(result.Fields, result.FieldTypes)); err != nil {
			return fmt.Errorf("write row description: %w", err)
		}
		for _, row := range result.Rows {
			if _, err := conn.Write(buildDataRow(row)); err != nil {
				return fmt.Errorf("write data row: %w", err)
			}
		}
	}

	// CommandComplete
	if _, err := conn.Write(buildCommandComplete(result.Tag)); err != nil {
		return fmt.Errorf("write command complete: %w", err)
	}

	// ReadyForQuery
	ready := NewMessage(ReadyForQuery, []byte("I"))
	if _, err := conn.Write(ready.ToByte()); err != nil {
		return fmt.Errorf("write ready for query: %w", err)
	}
	return nil
}
