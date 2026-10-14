// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package logstream

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func jsonLine(level, message string) string {
	return fmt.Sprintf(`{"level":%q,"message":%q}`, level, message)
}

// drain reads every currently queued event without blocking.
func drain(ch <-chan Event) (lines []Line, gaps []Gap) {
	for {
		select {
		case ev := <-ch:
			switch ev.Kind {
			case EventLine:
				lines = append(lines, ev.Line)
			case EventGap:
				gaps = append(gaps, ev.Gap)
			}
		default:
			return lines, gaps
		}
	}
}

func TestHub_Write(t *testing.T) {
	hub := NewHub(10)

	for i := range 5 {
		hub.Write("line " + string(rune('0'+i)))
	}

	if hub.Count() != 5 {
		t.Errorf("expected count 5, got %d", hub.Count())
	}
}

func TestHub_SequentialNumbers(t *testing.T) {
	hub := NewHub(100)

	hub.Write("a")
	hub.Write("b")
	hub.Write("c")

	history := hub.History(10)
	if len(history) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(history))
	}
	for i, line := range history {
		if want := uint64(i + 1); line.Seq != want {
			t.Errorf("line %d: expected seq %d, got %d", i, want, line.Seq)
		}
	}
}

func TestHub_RingBuffer(t *testing.T) {
	hub := NewHub(5)

	for i := range 10 {
		hub.Write("line " + string(rune('0'+i)))
	}

	if hub.Count() != 5 {
		t.Errorf("expected count 5, got %d", hub.Count())
	}

	history := hub.History(10) // Request more than available
	if len(history) != 5 {
		t.Fatalf("expected 5 lines, got %d", len(history))
	}

	// Content is the last 5 lines and seqs stay contiguous in write order.
	for i, line := range history {
		wantText := "line " + string(rune('5'+i))
		if line.Text != wantText {
			t.Errorf("expected %q, got %q", wantText, line.Text)
		}
		if wantSeq := uint64(6 + i); line.Seq != wantSeq {
			t.Errorf("expected seq %d, got %d", wantSeq, line.Seq)
		}
	}
}

func TestHub_HistoryPartial(t *testing.T) {
	hub := NewHub(100)

	for i := range 10 {
		hub.Write("line " + string(rune('0'+i)))
	}

	history := hub.History(3)
	if len(history) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(history))
	}

	expected := []string{"line 7", "line 8", "line 9"}
	for i, line := range history {
		if line.Text != expected[i] {
			t.Errorf("expected %q, got %q", expected[i], line.Text)
		}
		if wantSeq := uint64(8 + i); line.Seq != wantSeq {
			t.Errorf("expected seq %d, got %d", wantSeq, line.Seq)
		}
	}
}

func TestHub_Subscribe(t *testing.T) {
	hub := NewHub(100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sub := hub.Subscribe(ctx)

	hub.Write("test line")

	select {
	case ev := <-sub.Channel():
		if ev.Kind != EventLine {
			t.Fatalf("expected line event, got kind %d", ev.Kind)
		}
		if ev.Line.Text != "test line" {
			t.Errorf("expected 'test line', got %q", ev.Line.Text)
		}
		if ev.Line.Seq != 1 {
			t.Errorf("expected seq 1, got %d", ev.Line.Seq)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for line")
	}
}

func TestHub_Unsubscribe(t *testing.T) {
	hub := NewHub(100)
	ctx := context.Background()

	sub := hub.Subscribe(ctx)

	if hub.SubscriberCount() != 1 {
		t.Errorf("expected 1 subscriber, got %d", hub.SubscriberCount())
	}

	hub.Unsubscribe(sub)

	if hub.SubscriberCount() != 0 {
		t.Errorf("expected 0 subscribers, got %d", hub.SubscriberCount())
	}

	_, ok := <-sub.Channel()
	if ok {
		t.Error("expected channel to be closed")
	}
}

func TestHub_ContextCancel(t *testing.T) {
	hub := NewHub(100)
	ctx, cancel := context.WithCancel(context.Background())

	sub := hub.Subscribe(ctx)

	if hub.SubscriberCount() != 1 {
		t.Errorf("expected 1 subscriber, got %d", hub.SubscriberCount())
	}

	cancel()

	if !eventually(func() bool { return hub.SubscriberCount() == 0 }) {
		t.Errorf("expected 0 subscribers after context cancel, got %d", hub.SubscriberCount())
	}

	select {
	case <-sub.Done():
	default:
		t.Error("expected Done channel to be closed")
	}
}

func eventually(fn func() bool) bool {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return fn()
}

func TestHub_SlowConsumer(t *testing.T) {
	hub := NewHub(1000)
	sub, _ := hub.SubscribeWith(t.Context(), SubscribeOptions{})
	defer hub.Unsubscribe(sub)

	// 100 fit the channel; lines 101..150 are dropped and await a gap event.
	for range 150 {
		hub.Write("line")
	}

	lines, gaps := drain(sub.Channel())
	if len(lines) != DefaultSubscriberBuffer {
		t.Errorf("expected %d queued lines, got %d", DefaultSubscriberBuffer, len(lines))
	}
	if len(gaps) != 0 {
		t.Errorf("gap must not overtake buffered lines, got %d gaps", len(gaps))
	}

	// The next write has room: it first flushes the gap, then delivers itself.
	hub.Write("after gap")
	select {
	case ev := <-sub.Channel():
		if ev.Kind != EventGap {
			t.Fatalf("expected gap event, got kind %d", ev.Kind)
		}
		if ev.Gap.From != 101 || ev.Gap.To != 150 || ev.Gap.Dropped != 50 {
			t.Errorf("unexpected first gap: %+v", ev.Gap)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for gap event")
	}
	select {
	case ev := <-sub.Channel():
		if ev.Kind != EventLine || ev.Line.Seq != 151 {
			t.Fatalf("expected line seq 151 after gap, got %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for line after gap")
	}

	// Refill and drop again: the count is cumulative across drop runs.
	for range 100 {
		hub.Write("line") // seqs 152..251
	}
	hub.Write("line") // 252 dropped
	hub.Write("line") // 253 dropped
	if lines, _ := drain(sub.Channel()); len(lines) != 100 {
		t.Fatalf("expected 100 queued lines, got %d", len(lines))
	}
	hub.Write("line") // 254: flushes the second gap and delivers

	select {
	case ev := <-sub.Channel():
		if ev.Kind != EventGap {
			t.Fatalf("expected second gap event, got kind %d", ev.Kind)
		}
		if ev.Gap.From != 252 || ev.Gap.To != 253 || ev.Gap.Dropped != 52 {
			t.Errorf("unexpected second gap: %+v", ev.Gap)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for second gap event")
	}
}

func TestHub_ConcurrentWrite(t *testing.T) {
	hub := NewHub(1000)

	var wg sync.WaitGroup
	numWriters := 10
	linesPerWriter := 100

	for range numWriters {
		wg.Go(func() {
			for range linesPerWriter {
				hub.Write("line")
			}
		})
	}

	wg.Wait()

	if hub.Count() != 1000 {
		t.Errorf("expected count 1000, got %d", hub.Count())
	}

	// The buffer holds each sequence exactly once, in order.
	history := hub.History(0)
	if len(history) != 1000 {
		t.Fatalf("expected 1000 history lines, got %d", len(history))
	}
	for i, line := range history {
		if want := uint64(i + 1); line.Seq != want {
			t.Fatalf("expected contiguous seq %d, got %d (index %d)", want, line.Seq, i)
		}
	}
}

func TestHub_HistoryEmpty(t *testing.T) {
	hub := NewHub(100)

	if history := hub.History(10); history != nil {
		t.Errorf("expected nil history for empty hub, got %v", history)
	}
}

func TestHub_DefaultSize(t *testing.T) {
	hub := NewHub(0)

	for range DefaultBufferSize + 100 {
		hub.Write("line")
	}

	if hub.Count() != DefaultBufferSize {
		t.Errorf("expected count %d, got %d", DefaultBufferSize, hub.Count())
	}
}

func TestHub_Resume_ReplaysMissedSegment(t *testing.T) {
	hub := NewHub(100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	for i := range 10 {
		hub.Write(fmt.Sprintf("line-%d", i+1))
	}

	sub, snapshot := hub.SubscribeWith(ctx, SubscribeOptions{AfterSeq: 7})
	defer hub.Unsubscribe(sub)

	if snapshot.Gap != nil {
		t.Fatalf("did not expect a gap, got %+v", snapshot.Gap)
	}
	if len(snapshot.Lines) != 3 {
		t.Fatalf("expected 3 replayed lines, got %d", len(snapshot.Lines))
	}
	for i, line := range snapshot.Lines {
		if want := uint64(8 + i); line.Seq != want {
			t.Errorf("expected replayed seq %d, got %d", want, line.Seq)
		}
	}
	if snapshot.AnchorSeq != 10 {
		t.Errorf("expected anchor 10, got %d", snapshot.AnchorSeq)
	}

	// A line written after subscribe is delivered live and continues the seq.
	hub.Write("line-11")
	select {
	case ev := <-sub.Channel():
		if ev.Kind != EventLine || ev.Line.Seq != 11 {
			t.Fatalf("expected live line seq 11, got %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for live line")
	}
}

func TestHub_Resume_CaughtUp(t *testing.T) {
	hub := NewHub(100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	for range 10 {
		hub.Write("line")
	}

	sub, snapshot := hub.SubscribeWith(ctx, SubscribeOptions{AfterSeq: 10})
	defer hub.Unsubscribe(sub)

	if snapshot.Gap != nil || len(snapshot.Lines) != 0 {
		t.Fatalf("expected empty caught-up snapshot, got %+v", snapshot)
	}
}

func TestHub_Resume_SequenceEvicted(t *testing.T) {
	hub := NewHub(5)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	for range 10 {
		hub.Write("line") // buffered seqs 6..10
	}

	sub, snapshot := hub.SubscribeWith(ctx, SubscribeOptions{AfterSeq: 3})
	defer hub.Unsubscribe(sub)

	if snapshot.Gap == nil {
		t.Fatal("expected a gap for evicted sequences")
	}
	if snapshot.Gap.From != 4 || snapshot.Gap.To != 5 || snapshot.Gap.Dropped != 2 {
		t.Errorf("unexpected gap: %+v", snapshot.Gap)
	}
	if len(snapshot.Lines) != 5 {
		t.Fatalf("expected 5 buffered lines, got %d", len(snapshot.Lines))
	}
	for i, line := range snapshot.Lines {
		if want := uint64(6 + i); line.Seq != want {
			t.Errorf("expected buffered seq %d, got %d", want, line.Seq)
		}
	}
}

func TestHub_Resume_BoundaryNoGap(t *testing.T) {
	hub := NewHub(5)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	for range 10 {
		hub.Write("line") // oldest buffered seq = 6
	}

	_, snapshot := hub.SubscribeWith(ctx, SubscribeOptions{AfterSeq: 5})

	if snapshot.Gap != nil {
		t.Errorf("resume immediately before the oldest line must not gap, got %+v", snapshot.Gap)
	}
	if len(snapshot.Lines) != 5 || snapshot.Lines[0].Seq != 6 {
		t.Errorf("expected replay from seq 6, got %+v", snapshot.Lines)
	}
}

func TestHub_FreshSnapshot_Limit(t *testing.T) {
	hub := NewHub(100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	for range 6 {
		hub.Write("line")
	}

	_, snapshot := hub.SubscribeWith(ctx, SubscribeOptions{Limit: 3})
	if len(snapshot.Lines) != 3 {
		t.Fatalf("expected 3 snapshot lines, got %d", len(snapshot.Lines))
	}
	for i, line := range snapshot.Lines {
		if want := uint64(4 + i); line.Seq != want {
			t.Errorf("expected snapshot seq %d, got %d", want, line.Seq)
		}
	}
}

func TestHub_LevelFilter_Snapshot(t *testing.T) {
	hub := NewHub(100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	hub.Write(jsonLine("info", "info one"))   // seq 1
	hub.Write(jsonLine("warn", "warn one"))   // seq 2
	hub.Write(jsonLine("error", "err one"))   // seq 3
	hub.Write(jsonLine("debug", "debug one")) // seq 4
	hub.Write("unclassified line")            // seq 5, unknown level

	_, snapshot := hub.SubscribeWith(ctx, SubscribeOptions{MinLevel: LevelWarn})

	got := make([]uint64, 0, len(snapshot.Lines))
	for _, line := range snapshot.Lines {
		got = append(got, line.Seq)
	}
	want := []uint64{2, 3, 5}
	if len(got) != len(want) {
		t.Fatalf("expected seqs %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected seqs %v, got %v", want, got)
		}
	}
}

func TestHub_LevelFilter_Live(t *testing.T) {
	hub := NewHub(100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sub, _ := hub.SubscribeWith(ctx, SubscribeOptions{MinLevel: LevelWarn})
	defer hub.Unsubscribe(sub)

	hub.Write(jsonLine("trace", "trace one"))
	hub.Write(jsonLine("debug", "debug one"))
	hub.Write(jsonLine("info", "info one"))
	hub.Write(jsonLine("warn", "warn one"))
	hub.Write(jsonLine("error", "err one"))
	hub.Write("plain unclassified")

	lines, _ := drain(sub.Channel())
	if len(lines) != 3 {
		t.Fatalf("expected warn, error and unknown lines (3), got %d", len(lines))
	}
	if lines[0].Level != LevelWarn || lines[1].Level != LevelError || lines[2].Level != LevelUnknown {
		t.Errorf("unexpected levels: %v %v %v", lines[0].Level, lines[1].Level, lines[2].Level)
	}
}

func TestHub_SubscriberIsolation(t *testing.T) {
	hub := NewHub(1000)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	subA, _ := hub.SubscribeWith(ctx, SubscribeOptions{})                    // all levels, drained
	subB, _ := hub.SubscribeWith(ctx, SubscribeOptions{MinLevel: LevelWarn}) // slow, warn+

	var mu sync.Mutex
	var aSeqs []uint64
	aGaps := 0
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		for ev := range subA.Channel() {
			mu.Lock()
			if ev.Kind == EventLine {
				aSeqs = append(aSeqs, ev.Line.Seq)
			} else {
				aGaps++
			}
			mu.Unlock()
		}
	}()

	for range 150 {
		hub.Write(jsonLine("info", "info")) // seqs 1..150: filtered for B
	}
	for range 150 {
		hub.Write(jsonLine("warn", "warn")) // seqs 151..300: 100 queued for B, 50 dropped
	}

	if !eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(aSeqs) == 300
	}) {
		mu.Lock()
		got := len(aSeqs)
		mu.Unlock()
		t.Fatalf("fast subscriber expected 300 lines, got %d", got)
	}

	// B: exactly the 100 queued warn lines, no gap flushed while full. first.
	bLines, bGaps := drain(subB.Channel())
	if len(bLines) != 100 {
		t.Fatalf("expected 100 warn lines for B, got %d", len(bLines))
	}
	if bLines[0].Seq != 151 || bLines[99].Seq != 250 {
		t.Errorf("expected B seqs 151..250, got %d..%d", bLines[0].Seq, bLines[99].Seq)
	}
	if len(bGaps) != 0 {
		t.Errorf("expected no flushed gap while B is full, got %d", len(bGaps))
	}

	hub.Write(jsonLine("warn", "warn")) // seq 301: flushes B's gap then delivers
	select {
	case ev := <-subB.Channel():
		if ev.Kind != EventGap || ev.Gap.From != 251 || ev.Gap.To != 300 || ev.Gap.Dropped != 50 {
			t.Fatalf("unexpected B gap: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for B gap event")
	}
	select {
	case ev := <-subB.Channel():
		if ev.Kind != EventLine || ev.Line.Seq != 301 {
			t.Fatalf("expected B line seq 301, got %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for B line")
	}

	hub.Unsubscribe(subA)
	<-doneA

	mu.Lock()
	defer mu.Unlock()
	if aGaps != 0 {
		t.Errorf("fast subscriber must not see gaps, got %d", aGaps)
	}
	if len(aSeqs) != 301 {
		t.Errorf("fast subscriber expected 301 lines total, got %d", len(aSeqs))
	}
}
