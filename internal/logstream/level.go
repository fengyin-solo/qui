// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package logstream

import "strings"

// levelNames pairs zerolog level names (and console abbreviations, which only
// differ in length) with their Level, longest first where prefixes could clash.
var levelNames = []struct {
	name  string
	level Level
}{
	{"trace", LevelTrace},
	{"debug", LevelDebug},
	{"info", LevelInfo},
	{"warn", LevelWarn},
	{"error", LevelError},
	{"fatal", LevelFatal},
	{"panic", LevelPanic},
	{"trc", LevelTrace},
	{"dbg", LevelDebug},
	{"inf", LevelInfo},
	{"wrn", LevelWarn},
	{"err", LevelError},
	{"ftl", LevelFatal},
	{"pnc", LevelPanic},
}

// ParseLevel resolves a zerolog level name ("info", "WARN", ...) or console
// abbreviation ("INF", "wrn") to a Level. It returns LevelUnknown for anything
// unrecognized.
func ParseLevel(name string) Level {
	return parseLevelName(strings.TrimSpace(name))
}

func parseLevelName(s string) Level {
	for _, candidate := range levelNames {
		if equalFoldASCII(s, candidate.name) {
			return candidate.level
		}
	}
	return LevelUnknown
}

func equalFoldASCII(s, target string) bool {
	if len(s) != len(target) {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != target[i] {
			return false
		}
	}
	return true
}

// LevelFromText derives the level of a rendered log line. Production builds
// emit zerolog JSON ("level":"info"); dev builds emit ConsoleWriter lines of
// the form "<timestamp> INF message". Unparseable lines are LevelUnknown so
// filtering never hides output it cannot classify. It allocates nothing for
// well-formed input because it runs on the per-log-line hot path.
func LevelFromText(text string) Level {
	i := skipSpaces(text, 0)
	if i >= len(text) {
		return LevelUnknown
	}
	if text[i] == '{' {
		return levelFromJSON(text)
	}

	// ConsoleWriter: "<timestamp> <LEVEL> message"; tolerate a missing timestamp.
	start, end, ok := tokenBounds(text, i)
	if !ok {
		return LevelUnknown
	}
	if nextStart, nextEnd, hasNext := tokenBounds(text, end); hasNext {
		if level := parseConsoleToken(text[nextStart:nextEnd]); level != LevelUnknown {
			return level
		}
	}
	return parseConsoleToken(text[start:end])
}

// levelFromJSON finds the top-level "level" string value with a state machine
// that understands string/escape boundaries, so a level-looking token inside
// the message field cannot be mistaken for the real one.
func levelFromJSON(line string) Level {
	inString := false
	escaped := false
	depth := 0

	for i := 0; i < len(line); i++ {
		c := line[i]
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
		case ':':
			if depth == 1 && jsonKeyIs(line, i, "level") {
				if value := jsonStringValue(line, i+1); value != "" {
					return parseLevelName(value)
				}
			}
		}
	}
	return LevelUnknown
}

// jsonKeyIs reports whether the ':' at colon terminates a key of the given name.
func jsonKeyIs(line string, colon int, key string) bool {
	j := colon - 1
	for j >= 0 && line[j] == ' ' {
		j--
	}
	if j < 0 || line[j] != '"' {
		return false
	}
	start := j - len(key) - 1
	return start >= 0 && line[start] == '"' && line[start+1:j] == key
}

// jsonStringValue returns the quoted string value starting after a colon.
func jsonStringValue(line string, pos int) string {
	for pos < len(line) && line[pos] == ' ' {
		pos++
	}
	if pos >= len(line) || line[pos] != '"' {
		return ""
	}
	pos++
	start := pos
	for pos < len(line) {
		switch line[pos] {
		case '\\':
			pos++ // skip escaped byte
		case '"':
			return line[start:pos]
		}
		pos++
	}
	return ""
}

// tokenBounds returns the [start,end) byte range of the first space-delimited
// token at or after pos.
func tokenBounds(s string, pos int) (int, int, bool) {
	start := skipSpaces(s, pos)
	if start >= len(s) {
		return 0, 0, false
	}
	end := start
	for end < len(s) && s[end] != ' ' {
		end++
	}
	return start, end, true
}

func skipSpaces(s string, pos int) int {
	for pos < len(s) && s[pos] == ' ' {
		pos++
	}
	return pos
}

// parseConsoleToken matches a level token while ignoring SGR color escapes
// zerolog's ConsoleWriter wraps around the level when attached to a terminal.
func parseConsoleToken(token string) Level {
	for _, candidate := range levelNames {
		if tokenEqualsLevel(token, candidate.name) {
			return candidate.level
		}
	}
	return LevelUnknown
}

func tokenEqualsLevel(token, target string) bool {
	ti := 0
	for si := 0; si < len(token); si++ {
		if token[si] == 0x1b {
			// Skip the rest of the CSI sequence (terminated by 'm' for SGR).
			for si < len(token) && token[si] != 'm' {
				si++
			}
			continue
		}
		if ti >= len(target) {
			return false
		}
		c := token[si]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != target[ti] {
			return false
		}
		ti++
	}
	return ti == len(target)
}
