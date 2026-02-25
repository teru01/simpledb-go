package dbserver

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
)

func StartServer(ctx context.Context, addr string) error {
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
			if err := handleConn(ctx, conn); err != nil {
				slog.Error("connection error", "error", err)
			}
		}()
	}
}

func handleConn(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	slog.Info("new connection", "remote", conn.RemoteAddr())
	if err := handleInitializationMessage(conn); err != nil {
		return fmt.Errorf("handle initialization message: %w", err)
	}
	if err := handleQuery(ctx, conn); err != nil {
		return fmt.Errorf("handle query: %w", err)
	}
	slog.Info("connection closed", "remote", conn.RemoteAddr())
	return nil
}

func handleInitializationMessage(conn net.Conn) error {
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
			// return 'N'
			if _, err := conn.Write([]byte("N")); err != nil {
				return fmt.Errorf("respond N for ssl request: %w")
			}
		}
	} else {
		// StartupMessage
		startupPayload := make([]byte, messageLen-4)
		if _, err := io.ReadFull(conn, startupPayload); err != nil {
			return fmt.Errorf("read startup payload: %w", err)
		}
		ok := NewMessage(AuthenticationOK, []byte{0, 0, 0, 0})
		if _, err := conn.Write(ok.ToByte()); err != nil {
			return fmt.Errorf("write authentication ok: %w", err)
		}
		ready := NewMessage(ReadyForQuery, []byte("I"))
		if _, err := conn.Write(ready.ToByte()); err != nil {
			return fmt.Errorf("write ready for query: %w", err)
		}
	}
	return nil
}

func handleQuery(ctx context.Context, conn net.Conn) error {
	for {
		sql, err := readSQL(conn)
		if err != nil {
			return fmt.Errorf("get sql: %w", err)
		}
		if sql == "" {
			break
		}
		fmt.Println("query:", sql)
		// if err := db.Execute(ctx, sql); err != nil {
		// 	return fmt.Errorf("execute query: %w", err)
		// }
	}
	return nil
}

func readSQL(conn io.ReadWriter) (string, error) {
	identifierBuf := make([]byte, 1)
	n, err := conn.Read(identifierBuf)
	if err != nil {
		return "", fmt.Errorf("read identifier: %w", err)
	}
	if n != 1 {
		return "", fmt.Errorf("read identifier: expected 1 byte but got %d", n)
	}
	identifier := MessageIdentifier(rune(identifierBuf[0]))
	if identifier != Query {
		slog.Debug("unexpected identifier", slog.Any("identifier", string(identifierBuf)))
		errMsg := NewMessage(ErrorResponse, []byte{0})
		if _, err := conn.Write(errMsg.ToByte()); err != nil {
			return "", fmt.Errorf("write ready for query: %w", err)
		}
		return "", nil
	}
	var queryPayloadLen int32
	if err := binary.Read(conn, binary.BigEndian, &queryPayloadLen); err != nil {
		return "", fmt.Errorf("read query payload: %w", err)
	}
	payload := make([]byte, queryPayloadLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return "", fmt.Errorf("read payload: %w", err)
	}
	return strings.TrimSpace(string(payload)), nil
}
