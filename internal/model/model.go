package model

import (
	"errors"
	"fmt"
	"strconv"
)

const MaxNumber uint64 = 9007199254740991

var (
	ErrUnavailable = errors.New("target temporarily unavailable")
	ErrConflict    = errors.New("same version has conflicting state")
	ErrNotFound    = errors.New("target not found")
)

type Target struct {
	Type        int    `json:"type"`
	ID          string `json:"id"`
	Title       string `json:"title"`
	Summary     string `json:"summary"`
	Category    string `json:"category"`
	Initialized bool   `json:"-"`
	RedisRunID  string `json:"-"`
}

type State struct {
	Liked   bool   `json:"liked"`
	Count   string `json:"count"`
	Version string `json:"version"`
	Changed bool   `json:"changed"`
	Code    string `json:"code"`
}

type Relation struct {
	UserID  string
	Liked   bool
	Version uint64
}

type Event struct {
	SchemaVersion int    `json:"schema_version"`
	EventID       string `json:"event_id"`
	Generation    string `json:"generation"`
	StreamID      string `json:"stream_id"`
	UserID        string `json:"user_id"`
	TargetID      string `json:"target_id"`
	TargetType    int    `json:"target_type"`
	Action        string `json:"action"`
	Version       string `json:"version"`
}

// Decimal validates canonical decimal strings without converting IDs to floating point.
func Decimal(s string, max uint64, positive bool) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != s || n > max || (positive && n == 0) {
		return 0, fmt.Errorf("invalid decimal: %q", s)
	}
	return n, nil
}

func (e Event) Validate() error {
	if e.SchemaVersion != 1 || e.TargetType < 1 || e.TargetType > 3 || (e.Action != "LIKE" && e.Action != "UNLIKE") {
		return errors.New("invalid event schema, type or action")
	}
	for _, id := range []string{e.UserID, e.TargetID} {
		if _, err := Decimal(id, ^uint64(0), true); err != nil {
			return err
		}
	}
	if _, err := Decimal(e.Version, MaxNumber, true); err != nil {
		return err
	}
	var ms, seq string
	for i, c := range e.StreamID {
		if c == '-' {
			ms, seq = e.StreamID[:i], e.StreamID[i+1:]
			break
		}
	}
	if _, err := Decimal(ms, ^uint64(0), false); err != nil {
		return err
	}
	if _, err := Decimal(seq, ^uint64(0), false); err != nil {
		return err
	}
	if e.Generation == "" || e.EventID != fmt.Sprintf("%d:%s:%s:%s", e.TargetType, e.TargetID, e.Generation, e.StreamID) {
		return errors.New("invalid event identity")
	}
	return nil
}
