package barethoven

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

type APIVersion int

const (
	APIVersion0 APIVersion = iota
	APIVersion1
	APIVersion2
)

const (
	TLSNone     int = iota // cannot do TLS
	TLSOK                  // can do, but not required on my end
	TLSRequired            // TLS is required
)

const (
	// TODO: how do i want to define these sorts of templated responses...
	DirectorAuthOK     = "1000 OK auth\n"
	DirectorAuthFailed = "1999 Authorization failed.\n"

	DefaultHeartbeatInterval = 30 * time.Second
	HeaderLength             = 4

	maxUint32Plus1 int = math.MaxUint32 + 1
)

var (
	api2ResultKey = []byte(`result`)

	directorAuthOK     = []byte(DirectorAuthOK)
	directorAuthFailed = []byte(DirectorAuthFailed)
)

type (
	SocketConfig struct {
		Debug bool

		Host         string
		Port         int
		Console      string
		Password     string
		TimeLocation *time.Location

		DefaultCatalog    string
		DefaultAPIVersion APIVersion

		EnableTLS     bool
		RequireTLS    bool
		TLSVerifyPeer bool
		CAFile        string
		CertFile      string
		KeyFile       string
		AllowCNs      string

		HeartbeatInterval time.Duration

		CommandConfigProvider CommandConfigProviderFunc
		CommandProvider       CommandProvider
		ConversationProvider  ConversationFactoryFunc
	}

	Socket struct {
		mu           sync.Mutex
		log          Logger
		config       *SocketConfig
		conn         net.Conn
		conversation Conversation

		pctx   context.Context
		ctx    context.Context
		cancel context.CancelFunc

		openWG  *sync.WaitGroup
		closed  bool
		closeWG *sync.WaitGroup

		serverCanDoTLS    bool
		serverRequiresTLS bool

		commandQueue chan Command
		currentCmd   Command

		addr string
	}
)

func NewSocket(conf *SocketConfig) (*Socket, error) {
	return newSocket(context.Background(), conf)
}

func NewSocketWithContext(ctx context.Context, conf *SocketConfig) (*Socket, error) {
	return newSocket(ctx, conf)
}

func newSocket(ctx context.Context, conf *SocketConfig) (*Socket, error) {
	if conf.Host == "" {
		return nil, errors.New("\"Host\" cannot be empty")
	}
	if conf.Port == 0 {
		return nil, errors.New("\"Port\" cannot be empty")
	}
	if conf.Console == "" {
		return nil, errors.New("\"Console\" cannot be empty")
	}

	sock := &Socket{
		config: &SocketConfig{
			conf.Debug,
			conf.Host,
			conf.Port,
			conf.Console,
			conf.Password,
			nil,
			conf.DefaultCatalog,
			conf.DefaultAPIVersion,
			conf.EnableTLS,
			conf.RequireTLS,
			conf.TLSVerifyPeer,
			conf.CAFile,
			conf.CertFile,
			conf.KeyFile,
			conf.AllowCNs,
			conf.HeartbeatInterval,
			conf.CommandConfigProvider,
			conf.CommandProvider,
			conf.ConversationProvider,
		},
		pctx: ctx,
	}

	if conf.TimeLocation == nil {
		sock.config.TimeLocation = time.Local
	} else {
		tmp := *conf.TimeLocation
		sock.config.TimeLocation = &tmp
	}

	if sock.config.HeartbeatInterval == 0 {
		sock.config.HeartbeatInterval = DefaultHeartbeatInterval
	}

	if sock.config.CommandConfigProvider == nil {
		sock.config.CommandConfigProvider = NewCommandConfig
	}

	if sock.config.CommandProvider == nil {
		sock.config.CommandProvider = NewCommand
	}

	if sock.config.ConversationProvider == nil {
		sock.config.ConversationProvider = NewConversation
	}

	sock.addr = fmt.Sprintf("%s:%d", sock.config.Host, sock.config.Port)
	sock.log = newSocketLogger(sock.addr, sock.config.Console)

	return sock, nil
}

func (s *Socket) Config() SocketConfig {
	return *s.config
}

func (s *Socket) Address() string {
	return s.addr
}

func (s *Socket) Conversation() Conversation {
	return s.conversation
}

func (s *Socket) Open() error {
	return s.openSocket()
}

func (s *Socket) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.cancel()
	s.closeWG.Wait()
}

func (s *Socket) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Socket) SendCommand(command Command) (err error) {
	if command.Closed() {
		err = &ConversationConcludedError{}
	} else if s.Closed() {
		err = &SocketClosedError{}
		if cp, ok := command.(ConclusionFactoryProvider); ok {
			command.Speak(cp.ConclusionFactory()(SignalCommandFailed, err))
		} else {
			command.Speak(NewConclusion(SignalCommandFailed, err))
		}
	} else {
		select {
		case s.commandQueue <- command:
			if s.config.Debug {
				s.log.Printf("Queueing command \"%d\"...", command.ID)
			}
		default:
			err = &CommandQueueFullError{}
			if cp, ok := command.(ConclusionFactoryProvider); ok {
				command.Speak(cp.ConclusionFactory()(SignalCommandFailed, err))
			} else {
				command.Speak(NewConclusion(SignalCommandFailed, err))
			}
		}
	}

	return
}

func (s *Socket) SendCommandAndWait(command Command) (stmt Statement, err error) {
	if err = s.SendCommand(command); err == nil {
		for d := range command.Spoke() {
			if d.DialogType() == DialogConclusion {
				stmt = d.(*DefaultConclusion).Statement()
				err = d.(*DefaultConclusion).Err()
			}
		}
	}

	return
}

func (s *Socket) Send(ctx context.Context, command string, apiVersion APIVersion, bvfsUpdate string) (Statement, error) {
	cmd, err := s.config.CommandProvider(
		ctx,
		command,
		s.config.CommandConfigProvider(
			s.config.DefaultCatalog,
			apiVersion,
			DefaultQuestionTimeout,
			bvfsUpdate,
			s.config.ConversationProvider),
	)
	if err != nil {
		return nil, err
	}
	return s.SendCommandAndWait(cmd)
}

// read will attempt to read bytes off of the socket until n is reached or context is terminated
func (s *Socket) read(ctx context.Context, n int) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}

	var totalReceived, received int
	var err error
	var buff []byte
	msg := make([]byte, n, n)

	if s.config.Debug {
		s.log.Printf("Trying to read %d bytes from connection...", n)
	}

	for totalReceived < n {
		select {
		case <-ctx.Done():
			if s.config.Debug {
				s.log.Printf("read() context done: %s", ctx.Err())
			}
			return nil, newSocketReadError(ctx.Err())
		default:
			buff = make([]byte, n-totalReceived, n-totalReceived)
			received, err = s.conn.Read(buff)
			if nil != err {
				return nil, newSocketReadError(err)
			}

			if s.config.Debug {
				s.log.Printf("Read %d bytes", received)
			}

			for i := 0; i < received; i++ {
				msg[totalReceived+i] = buff[i]
			}

			totalReceived += received
		}
	}

	return msg, nil
}

func (s *Socket) write(ctx context.Context, stmt Statement) error {
	s.mu.Lock()
	if s.conn == nil {
		s.mu.Unlock()
		return newSocketWriteError(&SocketClosedError{})
	}
	s.mu.Unlock()

	var payload []byte

	select {
	case <-ctx.Done():
		if s.config.Debug {
			s.log.Printf("write() context done: %s", ctx.Err())
		}
		return newSocketWriteError(ctx.Err())
	default:
		switch stmt.StatementType() {
		case StatementSignal:
			if s.config.Debug {
				s.log.Printf("Attempting to send Signal: \"%s\"", stmt.(Signal))
			}
			msgLen := stmt.(Signal).Network()
			payload = make([]byte, 4, 4)
			binary.BigEndian.PutUint32(payload, msgLen)

		case StatementMessage:
			var i uint32
			if s.config.Debug {
				s.log.Printf("Attempting to send Message: \"%s\"", strings.TrimRight(stmt.(Message).String(), "\n"))
			}
			msgLen := uint32(len(stmt.(Message)))
			pktLen := msgLen + 4
			payload = make([]byte, pktLen, pktLen)
			binary.BigEndian.PutUint32(payload, msgLen)
			for ; i < msgLen; i++ {
				payload[i+4] = stmt.(Message)[i]
			}
		default:
			return NewUnknownTerminalError(fmt.Sprintf("write() only accepts Signal and Message Statements, \"%s\" not allowed", stmt.StatementType()))
		}

		if s.config.Debug {
			s.log.Printf("Attempting to write %d bytes: %v", len(payload), payload)
		}

		sentBytes, err := s.conn.Write(payload)
		if nil != err {
			return newSocketWriteError(err)
		}

		if s.config.Debug {
			s.log.Printf("Sent %d bytes", sentBytes)
		}

		return nil
	}
}

// receive assumes we are NOT trying to receive a typed message and that the first 4 bytes are indicating
// message length and not message type.
func (s *Socket) receive(ctx context.Context) ([]byte, error) {
	var msgLen int
	var err error
	var buff []byte

	buff, err = s.read(ctx, 4)
	if nil != err {
		return nil, err
	}
	msgLen = ntohl(buff)

	if msgLen > 0 {
		if s.config.Debug {
			s.log.Printf("Calculated message Length: %d", msgLen)
		}

		buff, err = s.read(ctx, msgLen)
		if nil != err {
			return nil, err
		}
	}

	if s.config.Debug {
		s.log.Printf("Received Message: %s", strings.TrimRight(string(buff), "\n"))
	}

	return buff, nil
}

// login has the following workflow:
// 1. Send "Hello" message -> receives challenge request
// 2. Send response to challenge -> receives "OK" or "FAIL"
// 3. Send challenge -> receives challenge response
// 4. Verify challenge response
func (s *Socket) login(ctx context.Context) error {
	var hello string
	var recv, challenge []byte
	var err error

	// construct hello message
	if s.config.Console == "" {
		hello = "Hello *UserAgent* calling\n"
	} else {
		hello = fmt.Sprintf("Hello %s calling\n", bashSpaces(s.config.Console))
	}

	if s.config.Debug {
		s.log.Printf("Hello message built: %s", hello)
	}

	err = s.write(ctx, Message(hello))
	if nil != err {
		s.log.Printf("Error sending Hello message: %s", err)
		return err
	}

	// try to get challenge request
	challenge, err = s.receive(ctx)
	if nil != err {
		s.log.Printf("Error receiving challenge request to login: %s", err)
		return err
	}

	if s.config.Debug {
		s.log.Printf("Challenge: %s", string(challenge))
	}
	// respond to challenge
	err = s.cramMD5Response(ctx, challenge)
	if nil != err {
		s.log.Printf("Error sending challenge response: %s", err)
		return err
	}

	// did we respond to the challenge ok?
	recv, err = s.receive(ctx)
	if nil != err {
		s.log.Printf("Error receiving challenge response response: %s", err)
		return err
	}

	if s.config.Debug {
		s.log.Printf("Cram MD5 Response: %s", recv)
	}

	// did we authenticate ourselves?
	if bytes.HasPrefix(recv, directorAuthFailed) {
		s.log.Printf("Our challenge response was invalid: %s", string(recv))
		return newInvalidClientChallengeResponseError(Message(recv))
	}

	// write our own challenge request
	if bytes.HasPrefix(recv, directorAuthOK) {
		return s.cramMD5Challenge(ctx)
	}

	return NewUnknownTerminalError(fmt.Sprintf("saw unexpected response to login attempt: %s", string(recv)))
}

// cramMD5Response is the first half of "login", and uses the standard base64 encoder
func (s *Socket) cramMD5Response(ctx context.Context, challenge []byte) error {
	chal := new(string)
	ssl := new(int)

	cnt, err := fmt.Sscanf(string(challenge), "auth cram-md5 %s ssl=%d", chal, ssl)
	if nil != err {
		return NewUnknownTerminalError(fmt.Sprintf("unable to extract cram-md5 challenge and ssl support from \"%s\"", string(challenge)))
	}
	if 2 != cnt {
		return NewUnknownTerminalError(fmt.Sprintf("unableto parse both challenge and ssl flag. Parsed: %d", cnt))
	}

	if s.config.Debug {
		s.log.Printf("Director challenge: %s; SSL: %d", *chal, *ssl)
	}

	switch *ssl {
	case TLSOK:
		s.serverCanDoTLS = true
		if s.config.Debug {
			s.log.Print("Director can use TLS")
		}
	case TLSRequired:
		s.serverCanDoTLS = true
		s.serverRequiresTLS = true
		if s.config.Debug {
			s.log.Print("Director requires TLS")
		}
	default:
		s.serverCanDoTLS = false
		s.serverRequiresTLS = false
		if s.config.Debug {
			s.log.Print("Director is incapable of using TLS")
		}
	}

	// and finally base64 encode the resulting hash
	resp := crammit(*chal, s.config.Password, true)

	if s.config.Debug {
		s.log.Printf("Our challenge response: %s", resp)
	}

	return s.write(ctx, Message(resp))
}

// cramMD5Challenge is the 2nd half of "login", and requires the use of the custom base64 encoder
func (s *Socket) cramMD5Challenge(ctx context.Context) error {
	var sslMethod int

	if s.config.RequireTLS {
		sslMethod = TLSRequired
	} else if s.config.EnableTLS {
		sslMethod = TLSOK
	} else {
		sslMethod = TLSNone
	}

	client := fmt.Sprintf("<%d.%d@barethoven-client>", rand.Uint32(), time.Now().In(s.config.TimeLocation).Unix())

	challenge := fmt.Sprintf("auth cram-md5 %s ssl=%d", client, sslMethod)
	if s.config.Debug {
		s.log.Printf("Challenge to Director: %s", challenge)
	}

	err := s.write(ctx, Message(challenge))
	if nil != err {
		return err
	}

	msg, err := s.receive(ctx)
	if nil != err {
		return err
	}

	msg = rtrimNullBytes(msg)
	strMsg := string(msg)

	expected := crammit(client, s.config.Password, false)
	if strMsg != expected {
		return newInvalidDirectorChallengeResponseError(expected, strMsg)
	}

	if s.config.Debug {
		s.log.Printf("Director Challenge Response: %s", strMsg)
	}

	err = s.write(ctx, Message(DirectorAuthOK))
	if nil != err {
		return err
	}

	return nil
}

func (s *Socket) specifyCatalog(ctx context.Context, catalog string) error {
	var err error
	s.currentCmd, err = s.newCommand(ctx, fmt.Sprintf("use catalog=%s", catalog))
	if err != nil {
		return NewUnknownTerminalError(err.Error())
	}

	err = s.write(s.currentCmd.Context(), Message(s.currentCmd.Command()))
	if err != nil {
		s.currentCmd = nil
		return err
	}

	var conclusion *DefaultConclusion

	for d := range s.currentCmd.Spoke() {
		if d.DialogType() == DialogConclusion {
			conclusion = d.(*DefaultConclusion)
		}
	}

	s.currentCmd = nil

	if conclusion.Err() != nil {
		return conclusion.Err()
	}

	if sig, ok := conclusion.stmt.(Signal); ok {
		return NewUnknownTerminalError(fmt.Sprintf("received Signal \"%s\" when expecting Message", sig))
	}

	if s.config.Debug {
		s.log.Printf("Specify Catalog response: %s", conclusion.stmt.(Message))
	}

	return nil
}

func (s *Socket) specifyAPIVersion(ctx context.Context, apiVersion APIVersion) error {
	var err error

	switch apiVersion {
	case APIVersion2:
		if s.config.Debug {
			s.log.Printf("Attempting to use API version \"%d\"", APIVersion2)
		}
		s.currentCmd, err = s.newCommand(ctx, fmt.Sprintf(".api %d compact=yes", APIVersion2))
		if err != nil {
			return NewUnknownTerminalError(fmt.Sprintf("Unable to create command to set APIVersion to 2: %s", err))
		}

	case APIVersion1:
		if s.config.Debug {
			s.log.Printf("Attempting to use API version \"%d\"", APIVersion1)
		}
		s.currentCmd, err = s.newCommand(ctx, fmt.Sprintf(".api %d", APIVersion1))
		if err != nil {
			return NewUnknownTerminalError(fmt.Sprintf("Unable to create command to set APIVersion to 1: %s", err))
		}
	case APIVersion0:
		if s.config.Debug {
			s.log.Printf("Attempting to use API version \"%d\"", APIVersion0)
		}
		s.currentCmd, err = s.newCommand(ctx, fmt.Sprintf(".api %d", APIVersion0))
		if err != nil {
			return NewUnknownTerminalError(fmt.Sprintf("Unable to creaet command to set APIVersion to 0: %s", err))
		}

	default:
		return NewUnknownTerminalError(fmt.Sprintf("\"%d\" is not a valid API version", apiVersion))
	}

	err = s.write(s.currentCmd.Context(), Message(s.currentCmd.Command()))
	if err != nil {
		return err
	}

	var conclusion *DefaultConclusion

	for d := range s.currentCmd.Spoke() {
		if d.DialogType() == DialogConclusion {
			conclusion = d.(*DefaultConclusion)
		}
	}

	s.currentCmd = nil

	if conclusion.Err() != nil {
		return conclusion.Err()
	}

	if sig, ok := conclusion.stmt.(Signal); ok {
		return NewUnknownTerminalError(fmt.Sprintf("received Signal \"%s\" when expecting Message", sig))
	}

	if apiVersion == APIVersion2 {
		if !bytes.Contains(conclusion.stmt.(Message), api2ResultKey) {
			return &API2NotAvailableError{}
		}
	}

	if s.config.Debug {
		s.log.Printf("API Version Set response: %s", conclusion.stmt.(Message))
	}

	return nil
}

func (s *Socket) openSocket() error {
	var err error

	s.mu.Lock()
	if s.conn != nil {
		s.mu.Unlock()
		return nil
	}

	if err := s.pctx.Err(); err != nil {
		s.mu.Unlock()
		return NewUnknownTerminalError(fmt.Sprintf("parent context is done: %s", err))
	}
	s.mu.Unlock()

	// TODO: This is probably all wrong...
	if s.config.RequireTLS || s.config.EnableTLS {
		if s.config.Debug {
			s.log.Print("Initializing connection using TLS...")
		}
		tlsConf := &tls.Config{
			InsecureSkipVerify: true, // TODO: Implement manual verification...
		}
		if s.config.CAFile != "" {
			certPool := x509.NewCertPool()
			caCert, err := ioutil.ReadFile(s.config.CAFile)
			if err != nil {
				return err
			}
			certPool.AppendCertsFromPEM(caCert)
			tlsConf.RootCAs = certPool
		}
		if s.config.CertFile != "" && s.config.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(s.config.CertFile, s.config.KeyFile)
			if err != nil {
				return err
			}
			tlsConf.Certificates = []tls.Certificate{cert}
		}
		tlsConf.BuildNameToCertificate()
		s.conn, err = tls.Dial("tcp", s.addr, tlsConf)
	} else {
		if s.config.Debug {
			s.log.Print("Initializing connection...")
		}
		s.conn, err = net.Dial("tcp", s.addr)
	}

	if err != nil {
		return err
	}

	s.ctx, s.cancel = context.WithCancel(s.pctx)
	s.conversation = s.Config().ConversationProvider()

	err = s.login(s.ctx)
	if err != nil {
		s.Close()
		return err
	}

	recv, err := s.receive(s.ctx)
	if err != nil {
		return err
	}

	if s.config.Debug {
		s.log.Printf("Login response: %s", string(recv))
	}

	s.mu.Lock()
	s.closed = false

	// make new command queue and start go routines
	s.commandQueue = make(chan Command, 100)
	s.openWG = new(sync.WaitGroup)
	s.openWG.Add(2)
	s.closeWG = new(sync.WaitGroup)
	go s.reader()
	go s.runner()

	s.openWG.Wait()
	s.mu.Unlock()

	if s.config.DefaultCatalog != "" {
		err = s.specifyCatalog(s.ctx, s.config.DefaultCatalog)
		if err != nil {
			s.cancel()
			return err
		}
	}

	if s.config.DefaultAPIVersion > APIVersion0 {
		err = s.specifyAPIVersion(s.ctx, s.config.DefaultAPIVersion)
		if err != nil {
			s.cancel()
			return err
		}
	}

	s.log.Print("Socket open")

	return err
}

func (s *Socket) executeBVFSUpdate(cmd Command) error {
	var err error

	command := fmt.Sprintf(".bvfs_update jobid=%s", cmd.Config().BVFSUpdate())
	s.currentCmd, err = s.newCommand(cmd.Context(), command)
	if err != nil {
		return NewUnknownTerminalError(fmt.Sprintf("Unable to create command to execute \"%s\": %s", command, err))
	}

	err = s.write(s.currentCmd.Context(), Message(s.currentCmd.Command()))
	if err != nil {
		s.currentCmd = cmd
		return err
	}

	var conclusion *DefaultConclusion

	for d := range s.currentCmd.Spoke() {
		if d.DialogType() == DialogConclusion {
			conclusion = d.(*DefaultConclusion)
		}
	}

	s.currentCmd = cmd

	if conclusion.Err() != nil {
		return conclusion.Err()
	}

	if sig, ok := conclusion.stmt.(Signal); ok {
		return NewUnknownTerminalError(fmt.Sprintf("received Signal \"%s\" when expecting Message", sig))
	}

	if s.config.Debug {
		s.log.Printf("bvfs_update response: %s", conclusion.stmt.(Message))
	}

	return nil
}

func (s *Socket) reader() {
	s.closeWG.Add(1)

	var err error
	var buff []byte
	var sig Signal
	var msgLen int

	s.openWG.Done()

Reader:
	for {
		select {
		case <-s.ctx.Done():
			if s.config.Debug {
				s.log.Printf("reader() socket context done: %s", s.ctx.Err())
			}
			break Reader
		default:
			if s.conn == nil {
				time.Sleep(100 * time.Microsecond)
				continue Reader
			}

			buff, err = s.read(s.ctx, HeaderLength)
			if err != nil {
				goto GetOut
			}

			msgLen = ntohl(buff)

			if msgLen == 0 {
				continue Reader
			}

			if msgLen > 0 {
				buff, err = s.read(s.ctx, msgLen)
				if err != nil {
					goto GetOut
				}
				if s.config.Debug {
					s.log.Printf("Received Message: %s", string(buff))
				}
				s.conversation.Speak(Message(buff))
				if s.currentCmd != nil {
					s.currentCmd.Speak(Message(buff))
				}
			} else {
				sig = Signal(msgLen)
				if s.config.Debug {
					s.log.Printf("Received Signal: %d %s", sig, sig)
				}
				s.conversation.Speak(sig)
				if s.currentCmd != nil {
					s.currentCmd.Speak(sig)
				}

				switch sig {
				case SignalHeartbeat:
					if s.config.Debug {
						s.log.Print("Heartbeat received, sending response...")
					}
					err = s.write(s.ctx, SignalHeartbeatResponse)
					if err != nil {
						goto GetOut
					}
				case SignalHeartbeatResponse:
					s.log.Print("Heartbeat response seen")
				case SignalSubPrompt, SignalSubCommandPrompt, SignalSelectInput, SignalTextInput, SignalYesNo:
					var qFactory QuestionFactoryFunc = NewQuestion
					var msg Message
					var response Statement
					var conv Conversation
					var ctx, qctx context.Context
					var qcancel context.CancelFunc

					if s.currentCmd == nil {
						if qp, ok := s.conversation.(QuestionFactoryProvider); ok {
							qFactory = qp.QuestionFactory()
						}
						conv = s.conversation
						ctx = s.ctx
						qctx, qcancel = context.WithTimeout(s.currentCmd.Context(), DefaultQuestionTimeout)
					} else {
						if qp, ok := s.currentCmd.(QuestionFactoryProvider); ok {
							qFactory = qp.QuestionFactory()
						}
						msg = s.currentCmd.MessageBuffer()
						s.currentCmd.ResetMessageBuffer()
						conv = s.currentCmd
						ctx = s.currentCmd.Context()
						qctx, qcancel = context.WithTimeout(s.currentCmd.Context(), s.currentCmd.Config().QuestionTimeout())
					}

					question := qFactory(sig, msg)

					err = conv.Speak(question)
					if err != nil {
						qcancel()
						goto GetOut
					}

					select {
					case <-qctx.Done():
						qcancel()
						err = NewQuestionIgnoredError(sig, msg)
						goto GetOut
					case response = <-question.Response():
					}

					qcancel()

					err = s.write(ctx, response)
					if err != nil {
						goto GetOut
					}
				}
			}

			continue Reader
		}

	GetOut:
		if err != nil {
			if te, ok := err.(Error); ok && te.Terminal() {
				s.log.Printf("reader() Terminal error seen, exiting: %s", te)
				if s.currentCmd != nil && !s.currentCmd.Closed() {
					if cp, ok := s.currentCmd.(ConclusionFactoryProvider); ok {
						s.currentCmd.Speak(cp.ConclusionFactory()(SignalTerminate, err))
					} else {
						s.currentCmd.Speak(NewConclusion(SignalTerminate, err))
					}
				}
				break Reader
			}
			s.log.Printf("reader() non-terminal error seen during read loop: %s", err)
		}
	}

	s.closeWG.Done()
}

func (s *Socket) runner() {
	hbt := time.NewTicker(s.config.HeartbeatInterval)

	s.openWG.Done()

Runner:
	for {
		select {
		case <-s.ctx.Done():
			if s.config.Debug {
				s.log.Printf("runner() socket context done: %s", s.ctx.Err())
			}
			break Runner
		case <-hbt.C:
			if s.config.Debug {
				s.log.Print("Attempting to send Heartbeat...")
			}
			err := s.write(s.ctx, SignalHeartbeat)
			if err != nil {
				s.log.Printf("Error seen while attempting to send Heartbeat: %s", err)
				break Runner
			}
		case cmd := <-s.commandQueue:
			var err error
			var cmdCatalog string
			var cmdAPIVersion APIVersion
			var shouldBreak bool

			if cmd.Closed() {
				s.log.Printf("(command: %d) Command conversation has been closed, will not process", cmd.ID())
				continue Runner
			}

			s.currentCmd = cmd

			if cmdCatalog = cmd.Config().Catalog(); cmdCatalog != "" && cmdCatalog != s.config.DefaultCatalog {
				if err = s.specifyCatalog(cmd.Context(), cmdCatalog); err != nil {
					if te, ok := err.(Error); ok && te.Terminal() {
						s.log.Printf("Terminal error seen while setting Catalog: %s", te)
						shouldBreak = true
					} else {
						s.log.Printf("Non-terminal error seen while setting Catalog: %s", err)
					}
					goto Conclude
				} else {
					s.currentCmd = cmd
				}
			}
			if cmdAPIVersion = cmd.Config().APIVersion(); cmdAPIVersion != s.config.DefaultAPIVersion {
				if err = s.specifyAPIVersion(cmd.Context(), cmdAPIVersion); err != nil {
					if te, ok := err.(Error); ok && te.Terminal() {
						s.log.Printf("Terminal error seen while setting APIVersion: %s", te)
						shouldBreak = true
					} else {
						s.log.Printf("Non-terminal error seen while setting APIVersion: %s", err)
					}
					goto Conclude
				} else {
					s.currentCmd = cmd
				}
			}
			if cmd.Config().BVFSUpdate() != "" {
				if err = s.executeBVFSUpdate(cmd); err != nil {
					if te, ok := err.(Error); ok && te.Terminal() {
						s.log.Printf("Terminal error seen while executing BVFS Update: %s", te)
						shouldBreak = true
					} else {
						s.log.Printf("Non-terminal error seen while executing BVFS Update: %s", err)
					}
					goto Conclude
				}
			}

			if err = s.write(cmd.Context(), Message(cmd.Command())); err != nil {
				if te, ok := err.(Error); ok && te.Terminal() {
					shouldBreak = true
				}
				goto Conclude
			}

		Conclude:
			if err != nil {
				if cp, ok := cmd.(ConclusionFactoryProvider); ok {
					cmd.Speak(cp.ConclusionFactory()(SignalCommandFailed, err))
				} else {
					cmd.Speak(NewConclusion(SignalCommandFailed, err))
				}
				if shouldBreak {
					break Runner
				}
			}

			<-cmd.Finished()
			s.currentCmd = nil

			// TODO: Should these always be considered terminal errors?
			// TODO: Something other than ctx err checking to determine if we should execute...

			if s.ctx.Err() == nil {
				if cmdCatalog != "" && cmdCatalog != s.config.DefaultCatalog {
					if err = s.specifyCatalog(s.ctx, s.config.DefaultCatalog); err != nil {
						if te, ok := err.(Error); ok && te.Terminal() {
							s.log.Printf("Terminal error seen setting Catalog back to \"%d\" after command: %s", s.config.DefaultCatalog, te)
							break Runner
						} else {
							s.log.Printf("Non-terminal error seen setting Catalog back to \"%s\" after command: %s", s.config.DefaultCatalog, err)
						}
					}
				}
			}
			if s.ctx.Err() == nil {
				if cmdAPIVersion != s.config.DefaultAPIVersion {
					if err = s.specifyAPIVersion(s.ctx, s.config.DefaultAPIVersion); err != nil {
						if te, ok := err.(Error); ok && te.Terminal() {
							s.log.Printf("Terminal error seen setting APIVersion back to \"%d\" after command: %s", s.config.DefaultAPIVersion, te)
							break Runner
						} else {
							s.log.Printf("Non-terminal error seen setting APIVersion back to \"%s\" after command: %s", s.config.DefaultAPIVersion, err)
						}
					}
				}
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.log.Print("Closing socket...")

	s.cancel()
	hbt.Stop()

	s.closed = true
	err := s.conn.Close()
	if err != nil {
		s.log.Printf("Error seen while closing socket: %s", err)
	}
	close(s.commandQueue)

	if cp, ok := s.conversation.(ConclusionFactoryProvider); ok {
		s.conversation.Speak(cp.ConclusionFactory()(SignalTerminate, err))
	} else {
		s.conversation.Speak(NewConclusion(SignalTerminate, err))
	}
	if s.currentCmd != nil {
		if !s.currentCmd.Closed() {
			if cp, ok := s.currentCmd.(ConclusionFactoryProvider); ok {
				s.currentCmd.Speak(cp.ConclusionFactory()(SignalTerminate, err))
			} else {
				s.currentCmd.Speak(NewConclusion(SignalTerminate, err))
			}
		}
		s.currentCmd = nil
	}

	s.closeWG.Wait()

	// nil out conn after wg ended so we aren't trying to read from nil...
	s.conn = nil
}
