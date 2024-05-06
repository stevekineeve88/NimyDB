package util

import (
	"log/slog"
	"os"
)

type logLevel string

const (
	Info  logLevel = "info"
	Error logLevel = "error"
	Debug logLevel = "debug"
	Warn  logLevel = "warn"
)

type Logger struct {
	slog *slog.Logger
}

var logger *Logger

func GetLogger() Logger {
	if logger != nil {
		return *logger
	}
	logger = &Logger{
		slog: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}
	return *logger
}

func (l Logger) Log(message string, level logLevel, args ...any) {
	switch level {
	case Info:
		logger.slog.Info(message, args...)
	case Warn:
		logger.slog.Warn(message, args...)
	case Debug:
		logger.slog.Debug(message, args...)
	case Error:
		logger.slog.Error(message, args...)
	}
}
