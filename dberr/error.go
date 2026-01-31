package dberr

import (
	"errors"
	"fmt"
	"log/slog"
)

type Code string

const (
	CodeTransactionLockWaitAbort Code = "TRANSACTION_LOCK_WAIT_ABORT"
	CodeBufferWaitAbort          Code = "BUFFER_WAIT_ABORT"
	CodeSyntaxError              Code = "SYNTAX_ERROR"
)

type DBError struct {
	Code    Code
	Message string
	Err     error
}

func (e *DBError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *DBError) Unwrap() error {
	return e.Err
}

func New(code Code, message string, err error) error {
	return &DBError{Code: code, Message: message, Err: err}
}

func HandleErrorLog(logger *slog.Logger, err error) {
	var dbErr *DBError
	if errors.As(err, &dbErr) {
		logger.Error("DB Error", slog.String("code", string(dbErr.Code)), slog.String("message", dbErr.Message), slog.Any("error", dbErr.Err))
	} else {
		logger.Error("Unexpected Error", slog.Any("error", err))
	}
}
