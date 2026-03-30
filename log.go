package acropora

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
)

var (
	logger zerolog.Logger
)

func init() {
	// Default logger that writes to stdout with timestamps and Version prefix
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
		FormatMessage: func(i interface{}) string {
			return fmt.Sprintf("acrpra[%s] %v", Version, i)
		},
	}
	logger = zerolog.New(output).With().Timestamp().Logger()
}

// getLogger returns a sublogger from the context if one exists, otherwise returns the global logger.
func getLogger(ctx context.Context) *zerolog.Logger {
	l := zerolog.Ctx(ctx)
	if l.GetLevel() == zerolog.Disabled {
		return &logger
	}
	sub := l.With().Logger()
	return &sub
}

func info(ctx context.Context, format string, v ...interface{}) {
	getLogger(ctx).Info().Msgf(format, v...)
}

func errorf(ctx context.Context, format string, v ...interface{}) {
	getLogger(ctx).Error().Msgf(format, v...)
}

func debug(ctx context.Context, format string, v ...interface{}) {
	getLogger(ctx).Debug().Msgf(format, v...)
}
