// Copyright 2026 WoozyMasta
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"golang.org/x/term"
)

const (
	// DefaultTimeFormat is the default RFC3339-style timestamp layout for logs.
	DefaultTimeFormat = "2006-01-02T15:04:05.000Z07:00"
)

// Options configures the global zerolog logger used by Hysteron binaries.
type Options struct {
	// Level is trace, debug, info, warn, or error.
	Level string
	// Format is text (console) or json.
	Format string
	// Output is stdout, stderr, or a filesystem path.
	Output string
	// FileMode is append or truncate when Output is a file path.
	FileMode string
	// TimeFormat is a Go time layout for the timestamp field.
	TimeFormat string
	// Color is auto, always, or never. Applies to text format only.
	Color string
}

// Configure builds the global logger from Options. Call once per process
// after CLI parse. Returns a closer for file outputs; defer closer.Close().
func Configure(o Options) (io.Closer, error) {
	lvl, err := zerolog.ParseLevel(strings.ToLower(strings.TrimSpace(o.Level)))
	if err != nil || lvl == zerolog.NoLevel {
		if strings.TrimSpace(o.Level) == "" {
			lvl = zerolog.InfoLevel
		} else {
			return nil, fmt.Errorf("invalid log level %q", o.Level)
		}
	}

	format := strings.ToLower(strings.TrimSpace(o.Format))
	if format == "" {
		format = "text"
	}
	if format != "text" && format != "json" {
		return nil, fmt.Errorf("invalid log format %q (want text or json)", o.Format)
	}

	tfmt := resolveTimeFormat(o.TimeFormat)
	zerolog.TimeFieldFormat = tfmt

	w, closer, err := resolveOutputWriter(o.Output, o.FileMode)
	if err != nil {
		return nil, err
	}

	var out = w
	if format == "text" {
		out = zerolog.ConsoleWriter{
			Out:        w,
			TimeFormat: tfmt,
			NoColor:    colorNoTTY(o.Color, w),
		}
	}

	root := zerolog.New(out).Level(lvl).With().Timestamp().Logger()
	zlog.Logger = root
	zerolog.SetGlobalLevel(lvl)
	return closer, nil
}

// SetDebug sets the global minimum level to debug.
func SetDebug() {
	SetLevel(zerolog.DebugLevel)
}

// SetLevel sets the global minimum log level on the root logger.
func SetLevel(lvl zerolog.Level) {
	zlog.Logger = zlog.Logger.Level(lvl)
	zerolog.SetGlobalLevel(lvl)
}

// IsDebug reports whether the effective global level includes debug.
func IsDebug() bool {
	return zerolog.GlobalLevel() <= zerolog.DebugLevel
}

// IsTrace reports whether the effective global level includes trace.
func IsTrace() bool {
	return zerolog.GlobalLevel() <= zerolog.TraceLevel
}

// nopCloser for stdout/stderr.
type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

// resolveOutputWriter opens the log destination and returns a writer plus a
// closer (nil for stdout/stderr — caller may close with nop).
func resolveOutputWriter(output, fileMode string) (io.Writer, io.Closer, error) {
	out := strings.ToLower(strings.TrimSpace(output))
	switch out {
	case "", "stderr":
		return os.Stderr, nopCloser{os.Stderr}, nil
	case "stdout":
		return os.Stdout, nopCloser{os.Stdout}, nil
	}

	flags := os.O_CREATE | os.O_WRONLY
	switch strings.ToLower(strings.TrimSpace(fileMode)) {
	case "", "append":
		flags |= os.O_APPEND
	case "truncate":
		flags |= os.O_TRUNC
	default:
		return nil, nil, fmt.Errorf("invalid log file mode %q (want append or truncate)", fileMode)
	}
	f, err := os.OpenFile(output, flags, 0600) //nolint:gosec // log file path is operator-controlled
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

// resolveTimeFormat maps common names (like zinit) to zerolog/time layouts; any
// other non-empty string is treated as a Go reference-time layout.
func resolveTimeFormat(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return DefaultTimeFormat
	case "rfc3339":
		return time.RFC3339
	case "rfc3339nano":
		return time.RFC3339Nano
	case "unix":
		return zerolog.TimeFormatUnix
	case "unixms":
		return zerolog.TimeFormatUnixMs
	case "unixmicro":
		return zerolog.TimeFormatUnixMicro
	case "unixnano":
		return zerolog.TimeFormatUnixNano
	default:
		return value
	}
}

// shouldColorAuto follows NO_COLOR / FORCE_COLOR and TTY detection (same idea as zinit).
func shouldColorAuto(w io.Writer) bool {
	if _, ok := os.LookupEnv("NO_COLOR"); ok {
		return false
	}
	if _, ok := os.LookupEnv("FORCE_COLOR"); ok {
		return true
	}
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fd := f.Fd()
	if fd > uintptr(math.MaxInt) {
		return false
	}
	return term.IsTerminal(int(fd))
}

// colorNoTTY decides ConsoleWriter NoColor from Color option, env, and TTY.
func colorNoTTY(color string, w io.Writer) bool {
	c := strings.ToLower(strings.TrimSpace(color))
	switch c {
	case "always":
		return false
	case "never":
		return true
	case "", "auto":
		return !shouldColorAuto(w)
	default:
		return true
	}
}
