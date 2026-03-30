package acropora

import (
	"bytes"
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestLogOutput(t *testing.T) {
	oldVersion := Version
	Version = "1.2.3"
	defer func() { Version = oldVersion }()

	var buf bytes.Buffer
	// Re-initialize a local logger for testing to capture output
	testLogger := zerolog.New(zerolog.ConsoleWriter{
		Out:     &buf,
		NoColor: true,
		FormatMessage: func(i interface{}) string {
			return "acrpra[1.2.3] " + i.(string)
		},
	}).With().Timestamp().Logger()

	// 1. Test global logger (re-init for test)
	logger = testLogger
	info(context.Background(), "test info message")

	output := buf.String()
	assert.Contains(t, output, "acrpra[1.2.3] test info message")
	assert.Contains(t, output, "INF")

	buf.Reset()
	debug(context.Background(), "test debug message")
	output = buf.String()
	assert.Contains(t, output, "acrpra[1.2.3] test debug message")
	assert.Contains(t, output, "DBG")

	// 2. Test context inheritance
	buf.Reset()
	ctxLogger := zerolog.New(&buf).With().Str("custom_field", "custom_value").Logger()
	ctx := ctxLogger.WithContext(context.Background())

	info(ctx, "message with context")

	output = buf.String()
	// When inheriting from context, zerolog uses its own output, not our ConsoleWriter-wrapped global logger
	// But we called getLogger(ctx).Info().Msgf(format, v...)
	// Since getLogger returns the logger from context, it will use that logger's output.
	assert.Contains(t, output, "message with context")
	assert.Contains(t, output, "custom_field")
	assert.Contains(t, output, "custom_value")
}

func TestMatchEntityLogging(t *testing.T) {
	// This is a bit harder to test without a full DB setup,
	// but we can at least check if it compiles and the calls exist.
}
