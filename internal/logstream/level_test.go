// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package logstream

import "testing"

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input string
		want  Level
	}{
		{"trace", LevelTrace},
		{"DEBUG", LevelDebug},
		{" info ", LevelInfo},
		{"warn", LevelWarn},
		{"error", LevelError},
		{"fatal", LevelFatal},
		{"panic", LevelPanic},
		{"TRC", LevelTrace},
		{"DBG", LevelDebug},
		{"INF", LevelInfo},
		{"WRN", LevelWarn},
		{"ERR", LevelError},
		{"FTL", LevelFatal},
		{"PNC", LevelPanic},
		{"", LevelUnknown},
		{"verbose", LevelUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ParseLevel(tt.input); got != tt.want {
				t.Errorf("ParseLevel(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestLevelFromText(t *testing.T) {
	tests := []struct {
		name string
		line string
		want Level
	}{
		{"json info", `{"level":"info","time":"2026-10-15T00:00:00Z","message":"hi"}`, LevelInfo},
		{"json error uppercase", `{"level":"ERROR","message":"boom"}`, LevelError},
		{"json unknown level", `{"level":"verbose","message":"x"}`, LevelUnknown},
		{"json malformed", `{"level":"info"`, LevelInfo},
		{"json no level field", `{"message":"whatever"}`, LevelUnknown},
		// A level-looking string nested in the message must not override the
		// top-level field.
		{"json nested in message", `{"level":"info","message":"{\"level\":\"error\"}"}`, LevelInfo},
		{"console inf", "2026-10-15T12:00:00Z INF starting up", LevelInfo},
		{"console wrn", "2026-10-15T12:00:00Z WRN careful", LevelWarn},
		{"console colored", "\x1b[33m2026-10-15T12:00:00Z\x1b[0m \x1b[31mERR\x1b[0m failed", LevelError},
		{"console no timestamp", "ERR failed", LevelError},
		{"plain", "some plain output", LevelUnknown},
		{"empty", "", LevelUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LevelFromText(tt.line); got != tt.want {
				t.Errorf("LevelFromText() = %d, want %d", got, tt.want)
			}
		})
	}
}
