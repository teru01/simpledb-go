package dbserver

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
)

func StartServer(addr string) error {
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
			if err := handleConn(conn); err != nil {
				slog.Error("connection error", "error", err)
			}
		}()
	}
}

func handleConn(conn net.Conn) error {
	defer conn.Close()
	slog.Info("new connection", "remote", conn.RemoteAddr())

	slog.Info("connection closed", "remote", conn.RemoteAddr())
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
		// 認証はないのですべて読み捨て
		
	}
}
