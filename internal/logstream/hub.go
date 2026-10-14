// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

// Package logstream provides a thread-safe log broadcasting system with a ring buffer
// for log history and SSE-based streaming to subscribers.
package logstream

import (
	"context"
	"sync"
)

const (
	// DefaultBufferSize is the default number of log lines to keep in the ring buffer.
	DefaultBufferSize = 1000
	// DefaultSubscriberBuffer is the buffer size for each subscriber's channel.
	DefaultSubscriberBuffer = 100
)

// Level is the severity of a log line, ordered from least to most severe.
// LevelUnknown means the level could not be parsed from the rendered line;
// such lines always pass a minimum-level filter so they are never silently lost.
type Level uint8

const (
	LevelUnknown Level = iota
	LevelTrace
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
	LevelPanic
)

// Line is a ring buffer entry: the rendered log text plus its monotonic sequence number.
type Line struct {
	Seq   uint64
	Level Level
	Text  string
}

// Gap describes a half-open run of sequence numbers a subscriber cannot receive.
// Dropped is the cumulative number of lines missed by this subscriber (evicted
// history plus slow-consumer drops).
type Gap struct {
	From    uint64 `json:"from"`
	To      uint64 `json:"to"`
	Dropped uint64 `json:"dropped"`
}

// EventKind discriminates the events delivered on a subscriber channel.
type EventKind uint8

const (
	EventLine EventKind = iota
	EventGap
)

// Event is delivered to a subscriber: either a new log line or an explicit
// gap notification following slow-consumer drops.
type Event struct {
	Kind EventKind
	Line Line
	Gap  Gap
}

// SubscribeOptions configures a subscription.
type SubscribeOptions struct {
	// MinLevel, when set to a level other than LevelUnknown, suppresses lines
	// below that severity. LevelUnknown sends every line.
	MinLevel Level
	// AfterSeq resumes from history: only buffered lines with a higher sequence
	// number are replayed. Zero means a fresh connection, which gets a bounded
	// snapshot instead.
	AfterSeq uint64
	// Limit caps the number of lines in a fresh snapshot (zero/all -> whole buffer).
	// It does not apply to a resume, which replays the full missed segment.
	Limit int
}

// Snapshot is the catch-up state computed atomically when subscribing: any
// evicted-history gap followed by the lines to replay before going live.
type Snapshot struct {
	Gap   *Gap
	Lines []Line
	// AnchorSeq is the sequence the subscriber is caught up to after the snapshot.
	// Every live event has a higher sequence number.
	AnchorSeq uint64
}

// Hub manages log broadcasting to subscribers with a ring buffer for history.
type Hub struct {
	mu          sync.RWMutex
	buffer      []Line
	bufferSize  int
	writePos    int
	count       int
	nextSeq     uint64
	subscribers map[*Subscriber]struct{}
}

// Subscriber represents a log stream subscriber with a buffered channel.
type Subscriber struct {
	ch     chan Event
	ctx    context.Context
	cancel context.CancelFunc

	minLevel Level
	// anchorSeq is the highest global sequence accounted for this subscriber,
	// including filtered-out lines and the end of a reported gap.
	anchorSeq uint64
	// dropped is the cumulative count of deliverable lines this subscriber missed.
	dropped uint64
	// gapPending tracks a run of drops not yet notified. gapFrom/gapTo are its
	// inclusive sequence range.
	gapPending bool
	gapFrom    uint64
	gapTo      uint64
}

// NewHub creates a new Hub with the specified buffer size.
// If size <= 0, DefaultBufferSize is used.
func NewHub(size int) *Hub {
	if size <= 0 {
		size = DefaultBufferSize
	}
	return &Hub{
		buffer:      make([]Line, size),
		bufferSize:  size,
		subscribers: make(map[*Subscriber]struct{}),
	}
}

// Write appends a log line to the ring buffer and broadcasts it to subscribers.
// The level is derived from the rendered line (zerolog JSON or console format).
// Lines that exceed a subscriber's channel capacity are counted and reported
// through a gap event instead of being silently dropped.
func (h *Hub) Write(text string) {
	line := Line{Text: text, Level: LevelFromText(text)}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.nextSeq++
	line.Seq = h.nextSeq

	h.buffer[h.writePos] = line
	h.writePos = (h.writePos + 1) % h.bufferSize
	if h.count < h.bufferSize {
		h.count++
	}

	// Broadcast to subscribers. Must be inside the lock to prevent
	// send-on-closed-channel panics when Unsubscribe runs concurrently.
	for sub := range h.subscribers {
		h.deliverLocked(sub, line)
	}
}

// newestSeqLocked returns the sequence of the last buffered line, or 0 when
// the buffer is empty.
func (h *Hub) newestSeqLocked() uint64 {
	return h.nextSeq
}

// oldestSeqLocked returns the sequence of the first buffered line, or 0 when
// the buffer is empty.
func (h *Hub) oldestSeqLocked() uint64 {
	if h.count == 0 {
		return 0
	}
	return h.nextSeq - uint64(h.count) + 1
}

// historyLocked copies the last n buffered lines in write order.
func (h *Hub) historyLocked(n int) []Line {
	if n <= 0 || n > h.count {
		n = h.count
	}
	if n == 0 {
		return nil
	}

	result := make([]Line, n)
	start := (h.writePos - n + h.bufferSize) % h.bufferSize
	for i := range n {
		result[i] = h.buffer[(start+i)%h.bufferSize]
	}
	return result
}

// History returns the last n lines from the ring buffer in write order.
// If n <= 0 or n > count, returns all available lines.
func (h *Hub) History(n int) []Line {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.historyLocked(n)
}

// passes reports whether a line at level l should reach a subscriber with the
// given minimum level. Unknown levels on either side pass: callers cannot
// filter on a severity they cannot determine.
func passes(minLevel, level Level) bool {
	if minLevel == LevelUnknown || level == LevelUnknown {
		return true
	}
	return level >= minLevel
}

// Subscribe creates a subscriber for every level with no history replay.
// New callers that need filtering or resume should use SubscribeWith.
func (h *Hub) Subscribe(ctx context.Context) *Subscriber {
	sub, _ := h.SubscribeWith(ctx, SubscribeOptions{})
	return sub
}

// SubscribeWith creates a subscriber and atomically computes its catch-up
// snapshot, so a line can neither be missed nor duplicated between the
// replayed history and live delivery.
func (h *Hub) SubscribeWith(ctx context.Context, opts SubscribeOptions) (*Subscriber, Snapshot) {
	subCtx, cancel := context.WithCancel(ctx)
	sub := &Subscriber{
		ch:       make(chan Event, DefaultSubscriberBuffer),
		ctx:      subCtx,
		cancel:   cancel,
		minLevel: opts.MinLevel,
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	var snapshot Snapshot
	newest := h.newestSeqLocked()
	if opts.AfterSeq > 0 {
		// Resume. The anchor starts at the client's last seen sequence so live
		// broadcasts older than it are ignored; replay then advances it.
		sub.anchorSeq = opts.AfterSeq
		if opts.AfterSeq < newest {
			if oldest := h.oldestSeqLocked(); opts.AfterSeq+1 < oldest {
				// The missed segment starts before the oldest buffered line:
				// everything up to oldest-1 has slid out of the buffer.
				missing := oldest - 1 - opts.AfterSeq
				sub.dropped = missing
				snapshot.Gap = &Gap{From: opts.AfterSeq + 1, To: oldest - 1, Dropped: missing}
			}
			for _, line := range h.historyLocked(h.count) {
				if line.Seq > opts.AfterSeq && passes(opts.MinLevel, line.Level) {
					snapshot.Lines = append(snapshot.Lines, line)
				}
			}
			sub.anchorSeq = newest
		}
	} else {
		// Fresh connection: replay the last Limit matching lines.
		for _, line := range h.historyLocked(h.count) {
			if passes(opts.MinLevel, line.Level) {
				snapshot.Lines = append(snapshot.Lines, line)
			}
		}
		if opts.Limit > 0 && len(snapshot.Lines) > opts.Limit {
			snapshot.Lines = snapshot.Lines[len(snapshot.Lines)-opts.Limit:]
		}
		sub.anchorSeq = newest
	}
	snapshot.AnchorSeq = sub.anchorSeq

	h.subscribers[sub] = struct{}{}

	// Auto-unsubscribe when context is done.
	go func() {
		<-subCtx.Done()
		h.Unsubscribe(sub)
	}()

	return sub, snapshot
}

// deliverLocked enqueues one live line for a subscriber. A full channel starts
// or extends a drop run; the gap event is flushed ahead of the next line once
// there is room for both, so a line always immediately follows its gap and a
// trickling drain cannot turn repeated gaps into line starvation.
func (h *Hub) deliverLocked(sub *Subscriber, line Line) {
	if line.Seq <= sub.anchorSeq || !passes(sub.minLevel, line.Level) {
		return
	}

	if sub.gapPending {
		if len(sub.ch)+1 < cap(sub.ch) {
			sub.ch <- Event{Kind: EventGap, Gap: Gap{
				From: sub.gapFrom, To: sub.gapTo, Dropped: sub.dropped,
			}}
			sub.gapPending = false
			sub.anchorSeq = sub.gapTo
		} else {
			sub.gapTo = line.Seq
			sub.dropped++
			return
		}
	}

	select {
	case sub.ch <- Event{Kind: EventLine, Line: line}:
		sub.anchorSeq = line.Seq
	default:
		sub.gapPending = true
		sub.gapFrom = sub.anchorSeq + 1
		sub.gapTo = line.Seq
		sub.dropped++
	}
}

// Unsubscribe removes a subscriber and closes its channel.
func (h *Hub) Unsubscribe(sub *Subscriber) {
	h.mu.Lock()
	if _, ok := h.subscribers[sub]; ok {
		delete(h.subscribers, sub)
		sub.cancel()
		close(sub.ch)
	}
	h.mu.Unlock()
}

// Channel returns the subscriber's event channel.
func (s *Subscriber) Channel() <-chan Event {
	return s.ch
}

// Done returns the subscriber's context done channel.
func (s *Subscriber) Done() <-chan struct{} {
	return s.ctx.Done()
}

// SubscriberCount returns the current number of subscribers.
func (h *Hub) SubscriberCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}

// Count returns the number of lines currently in the buffer.
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.count
}
