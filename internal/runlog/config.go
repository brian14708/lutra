package runlog

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// ConfigureFromEnv sets bounds for both log tail and run watch subscriptions.
func (s *Service) ConfigureFromEnv() error {
	for _, option := range []struct {
		name   string
		target *int
	}{
		{"LUTRA_LOG_TAIL_BATCH_SIZE", &s.TailBatchSize},
		{"LUTRA_LOG_MAX_RESPONSE_BUFFER", &s.MaxResponseBuffer},
	} {
		if text := os.Getenv(option.name); text != "" {
			value, err := strconv.Atoi(text)
			if err != nil || value < 1 || value > 10000 {
				return fmt.Errorf("%s must be between 1 and 10000", option.name)
			}
			*option.target = value
		}
	}
	for _, option := range []struct {
		name   string
		target *time.Duration
	}{
		{"LUTRA_LOG_RECONNECT_DELAY", &s.ReconnectDelay},
		{"LUTRA_LOG_RETENTION", &s.Retention},
		{"LUTRA_LOG_MAX_CURSOR_AGE", &s.MaxCursorAge},
	} {
		if text := os.Getenv(option.name); text != "" {
			value, err := time.ParseDuration(text)
			if err != nil || value < 0 {
				return fmt.Errorf("%s must be a nonnegative duration", option.name)
			}
			*option.target = value
		}
	}
	return nil
}
