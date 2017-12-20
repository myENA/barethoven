package barethoven

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultQuestionTimeout = time.Second
)

var cmdID uint64

type (
	CommandConfig interface {
		Catalog() string                // catalog to use for this command if different from socket default
		APIVersion() APIVersion         // api version to use for this command if different from socket default
		QuestionTimeout() time.Duration // amount of time to wait for an answer to a question from the director before timing out
		BVFSUpdate() string             // a single or comma-delimited list of job id's to execute .bvfs_update with prior to command execution
	}

	CommandConfigProviderFunc func(catalog string, apiVersion APIVersion, questionTimeout time.Duration, bvfsUpdate string, conversationProvider ConversationFactoryFunc) CommandConfig

	DefaultCommandConfig struct {
		mu                  sync.Mutex
		catalog             string
		apiVersion          APIVersion
		questionTimeout     time.Duration
		bvfsUpdate          string
		conversationFactory ConversationFactoryFunc
	}
)

func NewCommandConfig(catalog string, apiVersion APIVersion, questionTimeout time.Duration, bvfsUpdate string, conversationFactory ConversationFactoryFunc) CommandConfig {
	c := &DefaultCommandConfig{
		sync.Mutex{},
		catalog,
		apiVersion,
		questionTimeout,
		bvfsUpdate,
		nil,
	}

	if conversationFactory == nil {
		c.conversationFactory = NewConversation
	} else {
		c.conversationFactory = conversationFactory
	}

	return c
}

func (c DefaultCommandConfig) Catalog() string {
	c.mu.Lock()
	cat := c.catalog
	c.mu.Unlock()
	return cat
}

func (c DefaultCommandConfig) APIVersion() APIVersion {
	c.mu.Lock()
	av := c.apiVersion
	c.mu.Unlock()
	return av
}

func (c DefaultCommandConfig) QuestionTimeout() time.Duration {
	c.mu.Lock()
	at := c.questionTimeout
	c.mu.Unlock()
	return at
}

func (c DefaultCommandConfig) BVFSUpdate() string {
	c.mu.Lock()
	bu := c.bvfsUpdate
	c.mu.Unlock()
	return bu
}

func (c DefaultCommandConfig) ConversationFactory() ConversationFactoryFunc {
	c.mu.Lock()
	var cf ConversationFactoryFunc
	if c.conversationFactory == nil {
		cf = NewConversation
	} else {
		cf = c.conversationFactory
	}
	c.mu.Unlock()
	return cf
}

type (
	// Command represents a request you have for the director.  Once engaged, all interaction happens through the
	// Conversation.
	Command interface {
		Conversation
		ID() uint64                // this should be an auto-inc id of some sort
		Context() context.Context  // context denoting lifespan of command, will also be used to hang question contexts off of
		Config() CommandConfig     // this should return a copy of the config used for this command
		Command() string           // command to send to the director
		MessageBuffer() Message    // must return copy of current message buffer
		ResetMessageBuffer()       // must clear message buffer
		Finished() <-chan struct{} // must be closed when conversation has reached conclusion
	}

	// CommandProvider will be used to create new commands to do things like:
	// - Setting Catalog
	// - Setting APIVersion
	// - Executing .bvfs_update
	CommandProvider func(ctx context.Context, command string, conf CommandConfig) (Command, error)

	DefaultCommand struct {
		mu     sync.Mutex
		ctx    context.Context
		id     uint64
		closed bool

		command string

		config       CommandConfig
		conversation Conversation
		finished     chan struct{}

		messageBuffer Message
		commandSignal Signal
	}
)

// NewCommand will attempt to create a new command using values specified in configuration.
//
// Few things to note:
// - if Catalog is left empty, the default catalog for the socket will be used (if defined)
// - if APIVersion is not explicitly defined, it will default to APIVersion0 regardless of what default of socket is
// - if QuestionTimeout is not explicitly defined, it will default to value of DefaultQuestionTimeout
func NewCommand(ctx context.Context, command string, conf CommandConfig) (Command, error) {
	return newCommand(ctx, command, conf)
}

func (s *Socket) newCommand(ctx context.Context, command string) (Command, error) {
	return newCommand(
		ctx,
		command,
		&DefaultCommandConfig{
			sync.Mutex{},
			s.config.DefaultCatalog,
			s.config.DefaultAPIVersion,
			DefaultQuestionTimeout,
			"",
			s.config.ConversationProvider,
		},
	)
}

func newCommand(ctx context.Context, command string, conf CommandConfig) (*DefaultCommand, error) {
	if command == "" {
		return nil, errors.New("command cannot be empty")
	}

	config := new(DefaultCommandConfig)
	if conf != nil {
		config.catalog = conf.Catalog()
		config.apiVersion = conf.APIVersion()
		config.questionTimeout = conf.QuestionTimeout()
		config.bvfsUpdate = conf.BVFSUpdate()
	}

	if config.questionTimeout <= 0 {
		config.questionTimeout = DefaultQuestionTimeout
	}

	cmd := &DefaultCommand{
		ctx:           ctx,
		id:            cmdID,
		command:       command,
		config:        config,
		messageBuffer: make(Message, 0),
		finished:      make(chan struct{}),
	}

	if cp, ok := conf.(ConversationFactoryProvider); ok && cp != nil {
		cmd.conversation = cp.ConversationFactory()()
	} else {
		cmd.conversation = NewConversation()
	}

	cmdID = atomic.AddUint64(&cmdID, 1)

	return cmd, nil
}

func (c *DefaultCommand) QuestionFactory() QuestionFactoryFunc {
	c.mu.Lock()
	var qf QuestionFactoryFunc
	if qp, ok := c.conversation.(QuestionFactoryProvider); ok {
		qf = qp.QuestionFactory()
	} else {
		qf = NewQuestion
	}
	c.mu.Unlock()
	return qf
}

func (c *DefaultCommand) ConclusionFactory() ConclusionFactoryFunc {
	c.mu.Lock()
	var cf ConclusionFactoryFunc
	if cp, ok := c.conversation.(ConclusionFactoryProvider); ok {
		cf = cp.ConclusionFactory()
	} else {
		cf = NewConclusion
	}
	c.mu.Unlock()
	return cf
}

func (c DefaultCommand) ID() uint64 {
	c.mu.Lock()
	id := c.id
	c.mu.Unlock()
	return id
}

func (c DefaultCommand) Command() string {
	c.mu.Lock()
	cm := c.command
	c.mu.Unlock()
	return cm
}

func (c *DefaultCommand) Context() context.Context {
	c.mu.Lock()
	if c.ctx == nil {
		c.ctx = context.TODO()
	}
	ctx := c.ctx
	c.mu.Unlock()
	return ctx
}

func (c *DefaultCommand) Config() CommandConfig {
	c.mu.Lock()
	conf := c.config
	c.mu.Unlock()
	return conf
}

func (c *DefaultCommand) Finished() <-chan struct{} {
	c.mu.Lock()
	if c.finished == nil {
		c.finished = make(chan struct{})
	}
	f := c.finished
	c.mu.Unlock()
	return f
}

func (c DefaultCommand) Closed() bool {
	c.mu.Lock()
	b := c.closed
	c.mu.Unlock()
	return b
}

func (c *DefaultCommand) MessageBuffer() Message {
	c.mu.Lock()
	l := len(c.messageBuffer)
	tmp := make(Message, l, l)
	copy(tmp, c.messageBuffer)
	c.mu.Unlock()
	return tmp
}

func (c *DefaultCommand) ResetMessageBuffer() {
	c.mu.Lock()
	c.messageBuffer = make(Message, 0)
	c.mu.Unlock()
}

func (c *DefaultCommand) Spoke() <-chan Dialog {
	c.mu.Lock()
	s := c.conversation.Spoke()
	// TODO: do something if Spoke() returns nil...?
	c.mu.Unlock()
	return s
}

func (c *DefaultCommand) Speak(d Dialog) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return &ConversationConcludedError{}
	}

	var err error

	switch d.DialogType() {
	case DialogStatement:
		if s, ok := d.(Signal); ok {
			switch s {
			case SignalCommandBegin, SignalCommandOK, SignalCommandFailed, SignalInvalidCommand:
				c.commandSignal = s
			case SignalEOD, SignalEODPoll, SignalMainPrompt:
				switch c.commandSignal {
				case SignalCommandFailed:
					err = &CommandFailedError{}
				case SignalInvalidCommand:
					err = &CommandInvalidError{}
				}
				sigErr := c.conversation.Speak(s)
				if cp, ok := c.conversation.(ConclusionFactoryProvider); ok {
					c.conclude(cp.ConclusionFactory()(c.messageBuffer, err))
				} else {
					c.conclude(NewConclusion(c.messageBuffer, err))
				}
				err = sigErr

			default:
				err = c.conversation.Speak(s)
			}
		} else if m, ok := d.(Message); ok {
			c.messageBuffer = append(c.messageBuffer, m...)
			err = c.conversation.Speak(m)
		} else {
			err = c.conversation.Speak(d)
		}
	case DialogConclusion:
		err = c.conclude(d.(Conclusion))
	default:
		err = c.conversation.Speak(d)
	}

	c.mu.Unlock()

	return err
}

func (c *DefaultCommand) conclude(conclusion Conclusion) error {
	err := c.conversation.Speak(conclusion)
	c.closed = true
	close(c.finished)
	return err
}
