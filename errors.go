package barethoven

import "fmt"

type Error interface {
	error
	Terminal() bool
}

type SocketWriteError struct{ msg string }

func newSocketWriteError(err error) error { return &SocketWriteError{err.Error()} }
func (e *SocketWriteError) Error() string { return e.msg }
func (SocketWriteError) Terminal() bool   { return true }

type SocketReadError struct{ msg string }

func newSocketReadError(err error) error { return &SocketReadError{err.Error()} }
func (e *SocketReadError) Error() string { return e.msg }
func (SocketReadError) Terminal() bool   { return true }

type API2NotAvailableError struct{}

func (API2NotAvailableError) Error() string {
	return "API 2 not available on director. Please upgrade to version 15.2.2 or greater and/or compile with jansson support."
}
func (API2NotAvailableError) Terminal() bool { return false }

type ConnectionTerminatedError struct{}

func (ConnectionTerminatedError) Error() string  { return "director terminated connection" }
func (ConnectionTerminatedError) Terminal() bool { return true }

type CommandFailedError struct{}

func (CommandFailedError) Error() string  { return "command failed" }
func (CommandFailedError) Terminal() bool { return false }

type CommandInvalidError struct{}

func (CommandInvalidError) Error() string  { return "command invalid" }
func (CommandInvalidError) Terminal() bool { return false }

type SocketClosedError struct{}

func (SocketClosedError) Error() string  { return "socket closed" }
func (SocketClosedError) Terminal() bool { return true }

type ConversationConcludedError struct{}

func (ConversationConcludedError) Error() string  { return "conversation concluded" }
func (ConversationConcludedError) Terminal() bool { return false }

type DialogIgnoredError struct{}

func (DialogIgnoredError) Error() string  { return "conversation dialog channel not being read from" }
func (DialogIgnoredError) Terminal() bool { return true }

type CommandQueueFullError struct{}

func (CommandQueueFullError) Error() string  { return "command queue is full" }
func (CommandQueueFullError) Terminal() bool { return false }

type QuestionIgnoredError struct {
	s Signal
	m Message
}

func NewQuestionIgnoredError(s Signal, m Message) error { return &QuestionIgnoredError{s, m} }
func (e *QuestionIgnoredError) Error() string           { return fmt.Sprintf("question ignored: %s %s", e.s, e.m) }
func (QuestionIgnoredError) Terminal() bool             { return true }

type InvalidClientChallengeResponseError struct{ m Message }

func newInvalidClientChallengeResponseError(m Message) error {
	return &InvalidClientChallengeResponseError{m}
}
func (e *InvalidClientChallengeResponseError) Error() string { return e.m.String() }
func (InvalidClientChallengeResponseError) Terminal() bool   { return true }

type UnknownTerminalError struct{ msg string }

func NewUnknownTerminalError(msg string) error { return &UnknownTerminalError{msg} }
func (e *UnknownTerminalError) Error() string  { return e.msg }
func (UnknownTerminalError) Terminal() bool    { return true }

type InvalidDirectorChallengeResponseError struct {
	expected string
	seen     string
}

func newInvalidDirectorChallengeResponseError(e, s string) error {
	return &InvalidDirectorChallengeResponseError{e, s}
}
func (e *InvalidDirectorChallengeResponseError) Error() string {
	return fmt.Sprintf("expected len %d \"%s\", saw len %d \"%s\"", len(e.expected), e.expected, len(e.seen), e.seen)
}
func (InvalidDirectorChallengeResponseError) Terminal() bool { return true }
