package barethoven

import (
	"fmt"
	"math"
	"sync"
)

type DialogType uint8

const (
	DialogStatement DialogType = iota
	DialogQuestion
	DialogConclusion
)

func (dt DialogType) String() string {
	switch dt {
	case DialogStatement:
		return "Statement"
	case DialogQuestion:
		return "Question"
	case DialogConclusion:
		return "Conclusion"
	default:
		panic(fmt.Sprintf("Unknown DialogType seen: %d", dt))
	}
}

// Dialog represents information communicated to you from the Director
type Dialog interface {
	DialogType() DialogType
}

type StatementType uint16

const (
	StatementSignal StatementType = iota
	StatementMessage
)

func (st StatementType) String() string {
	switch st {
	case StatementSignal:
		return "Signal"
	case StatementMessage:
		return "Message"
	default:
		panic(fmt.Sprintf("Unknown StatementType seen: %d", st))
	}
}

// Statement represents any sort of i/o data to or from the Director.
type Statement interface {
	Dialog
	StatementType() StatementType
}

// Network represents a non-message bearing statement from or to the director, indicating some action
// is needed / happening / finished
type Signal int32

const (
	SignalEOD               Signal = -1  // End of data stream, new data may follow
	SignalEODPoll           Signal = -2  // End of data and poll all in one
	SignalStatus            Signal = -3  // Send full status
	SignalTerminate         Signal = -4  // Conversation terminated, doing close()
	SignalPoll              Signal = -5  // Poll request, I'm hanging on a read
	SignalHeartbeat         Signal = -6  // Heartbeat Response requested
	SignalHeartbeatResponse Signal = -7  // Only response permitted to HB
	SignalSubCommandPrompt  Signal = -8  // No longer used -- Prompt for subcommand
	SignalUTCBTime          Signal = -9  // Send UTC btime
	SignalBreak             Signal = -10 // Stop current command -- ctl-c
	SignalStartSelect       Signal = -11 // Start of a selection list
	SignalEndSelect         Signal = -12 // End of a select list
	SignalInvalidCommand    Signal = -13 // Invalid command sent
	SignalCommandFailed     Signal = -14 // Command failed
	SignalCommandOK         Signal = -15 // Command succeeded
	SignalCommandBegin      Signal = -16 // Start command execution
	SignalMessagesPending   Signal = -17 // Messages pending
	SignalMainPrompt        Signal = -18 // Server ready and waiting
	SignalSelectInput       Signal = -19 // Return selection input
	SignalWarningMessage    Signal = -20 // Warning message
	SignalErrorMessage      Signal = -21 // Error message -- command failed
	SignalInfoMessage       Signal = -22 // Info message -- status line
	SignalRunCommand        Signal = -23 // Run command follows
	SignalYesNo             Signal = -24 // Request yes no response
	SignalStartRestoreTree  Signal = -25 // Start restore tree mode
	SignalEndRestoreTree    Signal = -26 // End restore tree mode
	SignalSubPrompt         Signal = -27 // Indicate we are at a subprompt
	SignalTextInput         Signal = -28 // Get text input from user
)

func (s Signal) StatementType() StatementType {
	return StatementSignal
}

func (Signal) DialogType() DialogType {
	return DialogStatement
}

func (s Signal) Network() uint32 {
	return math.MaxUint32 + uint32(s) + 1
}

func (s Signal) String() string {
	switch s {
	case SignalEOD:
		return "EOD"
	case SignalEODPoll:
		return "EODPoll"
	case SignalStatus:
		return "Status"
	case SignalTerminate:
		return "Terminate"
	case SignalPoll:
		return "Poll"
	case SignalHeartbeat:
		return "Heartbeat"
	case SignalHeartbeatResponse:
		return "Heartbeat Response"
	case SignalSubCommandPrompt:
		return "Sub-Command Prompt"
	case SignalUTCBTime:
		return "UTC BTime"
	case SignalBreak:
		return "Break"
	case SignalStartSelect:
		return "Start Select"
	case SignalEndSelect:
		return "End Select"
	case SignalInvalidCommand:
		return "Command Invalid"
	case SignalCommandFailed:
		return "Command Failed"
	case SignalCommandOK:
		return "Command OK"
	case SignalCommandBegin:
		return "Command Begin"
	case SignalMessagesPending:
		return "Messages Pending"
	case SignalMainPrompt:
		return "Main Prompt"
	case SignalSelectInput:
		return "Select Input"
	case SignalWarningMessage:
		return "Warning Message"
	case SignalErrorMessage:
		return "Error Message"
	case SignalInfoMessage:
		return "Info Message"
	case SignalRunCommand:
		return "Run Command"
	case SignalYesNo:
		return "Yes No"
	case SignalStartRestoreTree:
		return "Start Restore Tree"
	case SignalEndRestoreTree:
		return "End Restore Tree"
	case SignalSubPrompt:
		return "Sub-Prompt"
	case SignalTextInput:
		return "Text Input"

	default:
		panic(fmt.Sprintf("Unknown Signal seen: %d", s))
	}
}

// Message represents payload from or to the Director
type Message []byte

func (Message) StatementType() StatementType {
	return StatementMessage
}

func (Message) DialogType() DialogType {
	return DialogStatement
}

func (m Message) String() string {
	return string(m)
}

type (
	Question interface {
		Dialog
		Signal() Signal
		Message() Message
		Respond(Statement)
		Response() <-chan Statement
	}

	QuestionFactoryFunc func(Signal, Message) Question

	QuestionFactoryProvider interface {
		QuestionFactory() QuestionFactoryFunc
	}

	DefaultQuestion struct {
		sig      Signal
		msg      Message
		response chan Statement
	}
)

func NewQuestion(s Signal, m Message) Question {
	return &DefaultQuestion{s, m, make(chan Statement)}
}

func (q *DefaultQuestion) Signal() Signal {
	return q.sig
}

func (q *DefaultQuestion) Message() Message {
	return q.msg
}

func (q *DefaultQuestion) Respond(stmt Statement) {
	q.response <- stmt
	close(q.response)
}

func (q *DefaultQuestion) Response() <-chan Statement {
	return q.response
}

func (DefaultQuestion) DialogType() DialogType {
	return DialogQuestion
}

type (
	Conclusion interface {
		Dialog
		Statement() Statement
		Err() error
		String() string
	}

	ConclusionFactoryFunc func(Statement, error) Conclusion

	ConclusionFactoryProvider interface {
		ConclusionFactory() ConclusionFactoryFunc
	}

	DefaultConclusion struct {
		stmt Statement
		err  error
	}
)

func NewConclusion(stmt Statement, err error) Conclusion {
	return &DefaultConclusion{stmt, err}
}

func (c *DefaultConclusion) Statement() Statement {
	return c.stmt
}

// Err will return whatever error (if any) the director responded with as part of the conclusion
func (c *DefaultConclusion) Err() error {
	return c.err
}

func (DefaultConclusion) DialogType() DialogType {
	return DialogConclusion
}

func (c *DefaultConclusion) String() string {
	if c.err == nil {
		if c.stmt != nil {
			switch c.stmt.StatementType() {
			case StatementSignal:
				return fmt.Sprintf("Conclusion Signal: %d %s", c.stmt, c.stmt)
			case StatementMessage:
				return fmt.Sprintf("Conclusion Message: %s", c.stmt)
			}
		}
		return "Conclusion empty"
	}
	return c.err.Error()
}

type (
	Conversation interface {
		Spoke() <-chan Dialog // this should be listened to by something
		Speak(Dialog) error   // must push Dialog part onto Dialog channel and must return terminal error if conversation is closed
		Closed() bool         // must return true if a conclusion has been reached
	}

	ConversationProvider interface {
		Conversation() Conversation
	}

	// ConversationFactoryFunc is any function that returns a Conversation
	ConversationFactoryFunc func() Conversation

	ConversationFactoryProvider interface {
		ConversationFactory() ConversationFactoryFunc
	}

	DefaultConversation struct {
		mu     sync.Mutex
		closed bool
		dialog chan Dialog
	}
)

func NewConversation() Conversation {
	return &DefaultConversation{
		dialog: make(chan Dialog, 100),
	}
}

func (c *DefaultConversation) QuestionFactory() QuestionFactoryFunc {
	return NewQuestion
}

func (c *DefaultConversation) ConclusionFactory() ConclusionFactoryFunc {
	return NewConclusion
}

func (c *DefaultConversation) Spoke() <-chan Dialog {
	return c.dialog
}

// speak expects the caller to be holding a mutex lock
func (c *DefaultConversation) Speak(d Dialog) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return &ConversationConcludedError{}
	}
	select {
	case c.dialog <- d:
		if d.DialogType() == DialogConclusion {
			close(c.dialog)
			c.closed = true
		}
		return nil
	default:
		return &DialogIgnoredError{}
	}
}

func (c *DefaultConversation) Closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
