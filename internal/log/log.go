// Copyright 2017 Sorint.lab
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

// Package log provides shared Stolon logging helpers.
package log

import (
	"fmt"
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	s      *zap.SugaredLogger
	sColor *zap.SugaredLogger
)

// default info level
var level = zap.NewAtomicLevelAt(zapcore.InfoLevel)

func init() {
	config := zap.Config{
		Level:             level,
		Development:       false,
		DisableStacktrace: true,
		Encoding:          "console",
		EncoderConfig:     zap.NewDevelopmentEncoderConfig(),
		OutputPaths:       []string{"stderr"},
		ErrorOutputPaths:  []string{"stderr"},
	}

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Errorf("failed to initialize logger: %v", err))
	}
	s = logger.Sugar()

	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	logger, err = config.Build()
	if err != nil {
		panic(fmt.Errorf("failed to initialize color logger: %v", err))
	}
	sColor = logger.Sugar()
}

// SetDebug sets the global log level to debug.
func SetDebug() {
	level.SetLevel(zapcore.DebugLevel)
}

// SetLevel sets the global log level.
func SetLevel(lvl zapcore.Level) {
	level.SetLevel(lvl)
}

// IsDebug reports whether the global log level is debug.
func IsDebug() bool {
	return level.Level() == zapcore.DebugLevel
}

// S returns the shared sugared logger.
func S() *zap.SugaredLogger {
	return s
}

// StdLog returns the shared logger as a standard library logger.
func StdLog() *log.Logger {
	return zap.NewStdLog(s.Desugar())
}

// SColor returns the shared colorized sugared logger.
func SColor() *zap.SugaredLogger {
	return sColor
}

// StdLogColor returns the colorized logger as a standard library logger.
func StdLogColor() *log.Logger {
	return zap.NewStdLog(sColor.Desugar())
}
