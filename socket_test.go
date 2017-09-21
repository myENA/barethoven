package barethoven_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/myENA/barethoven"
	"log"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

const (
	defaultCatalog = "MyCatalog"
)

var (
	defaultCatalogConfig = &barethoven.SocketConfig{DefaultCatalog: defaultCatalog}
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// newTestSocket takes a *testing.T in case i want to do something with it later....
func newTestSocket(_ *testing.T, c *barethoven.SocketConfig) (*barethoven.Socket, error) {
	def := &barethoven.SocketConfig{
		Host:     "bareos.local",
		Port:     9101,
		Console:  "badassadmin",
		Password: "secret",
		Debug:    true,
	}

	if c != nil {
		if c.Host != "" {
			def.Host = c.Host
		}
		if c.Port != 0 {
			def.Port = c.Port
		}
		if c.Console != "" {
			def.Console = c.Console
		}
		if c.Password != "" {
			def.Password = c.Password
		}
		if c.HeartbeatInterval != 0 {
			def.HeartbeatInterval = c.HeartbeatInterval
		}
		if c.TimeLocation != nil {
			def.TimeLocation = c.TimeLocation
		}
		if c.DefaultCatalog != "" {
			def.DefaultCatalog = c.DefaultCatalog
		}
	}

	sock, err := barethoven.NewSocket(def)
	if err != nil {
		return nil, fmt.Errorf("unable to create socket: %s", err)
	}

	err = sock.Open()
	if err != nil {
		return nil, fmt.Errorf("unable to open socket: %s", err)
	}

	return sock, nil
}

func newDefaultCommandConfig(apiVersion barethoven.APIVersion) barethoven.CommandConfig {
	return barethoven.NewCommandConfig("", apiVersion, barethoven.DefaultQuestionTimeout, "", nil)
}

func waitForConclusion(t *testing.T, cmd barethoven.Command) {
	var conclusion *barethoven.DefaultConclusion
	for d := range cmd.Spoke() {
		if sig, ok := d.(barethoven.Signal); ok {
			t.Logf("Director Signaled: (%d) %s", sig, sig)
		} else if msg, ok := d.(barethoven.Message); ok {
			t.Logf("Director Messaged: %s", msg)
		} else if c, ok := d.(*barethoven.DefaultConclusion); ok {
			conclusion = c
		}
	}

	if err := conclusion.Err(); err != nil {
		t.Logf("Unable to execute \"%s\" with apitype %d: %s", cmd.Command(), cmd.Config().APIVersion(), err)
		t.FailNow()
	}

	if conclusion.Statement() == nil {
		t.Log("Message contains no data!")
		t.FailNow()
	}

	if sig, ok := conclusion.Statement().(barethoven.Signal); ok {
		t.Logf("Received Signal \"%s\" when expecting Message", sig)
		t.FailNow()
	}

	msg, ok := conclusion.Statement().(barethoven.Message)
	if !ok || len(msg) == 0 {
		t.Log("Received empty conslusion")
		t.FailNow()
	}

	log.Printf("Command Conclusion: %s", msg)

	return
}

func TestNewDefaultSocket(t *testing.T) {
	sock, err := newTestSocket(t, defaultCatalogConfig)
	if err != nil {
		t.Logf(err.Error())
		t.FailNow()
	}

	sock.Close()

	if !sock.Closed() {
		t.Log("Socket should marked as closed")
		t.FailNow()
	}
}

func TestDefaultSocket_Send(t *testing.T) {
	t.Run("ValidCommandsSucceed", func(t *testing.T) {
		t.Run("APIVersion0", func(t *testing.T) {
			sock, err := newTestSocket(t, defaultCatalogConfig)
			if err != nil {
				t.Logf("Error seen while creating client: %s", err)
				t.FailNow()
			}
			defer sock.Close()

			stmt, err := sock.Send(context.Background(), "list jobs", barethoven.APIVersion0, "")
			if err != nil {
				t.Logf("Error seen: %s", err)
				t.FailNow()
			}
			if stmt == nil {
				t.Log("stmt should not be nil")
				t.FailNow()
			} else if s, ok := stmt.(barethoven.Signal); ok {
				t.Logf("Expected Message, saw Signal: %s", s)
				t.FailNow()
			} else if m, ok := stmt.(barethoven.Message); ok {
				if nil == m || len(m) == 0 {
					t.Log("stmt is a Message, but it is empty")
					t.FailNow()
				}
			} else {
				t.Logf("Expected to see Statement of type Signal or Message, saw \"%s\"", reflect.TypeOf(stmt))
				t.FailNow()
			}
		})

		t.Run("APIVersion1", func(t *testing.T) {
			sock, err := newTestSocket(t, defaultCatalogConfig)
			if err != nil {
				t.Logf("Error seen while creating client: %s", err)
				t.FailNow()
			}
			defer sock.Close()

			stmt, err := sock.Send(context.Background(), "list jobs", barethoven.APIVersion1, "")
			if err != nil {
				t.Logf("Error seen: %s", err)
				t.FailNow()
			}
			if stmt == nil {
				t.Logf("stmt should not be nil")
				t.FailNow()
			} else if s, ok := stmt.(barethoven.Signal); ok {
				t.Logf("Expected Message, saw Signal: %s", s)
				t.FailNow()
			} else if m, ok := stmt.(barethoven.Message); ok {
				if m == nil || len(m) == 0 {
					t.Log("stmt is a Message, but it is empty")
					t.FailNow()
				}
			} else {
				t.Logf("Expected to see Statement of type Signal or Message, saw \"%s\"", reflect.TypeOf(stmt))
				t.FailNow()
			}
		})

		t.Run("APIVersion2", func(t *testing.T) {
			sock, err := newTestSocket(t, defaultCatalogConfig)
			if err != nil {
				t.Logf("Error seen while creating client: %s", err)
				t.FailNow()
			}
			defer sock.Close()

			stmt, err := sock.Send(context.Background(), "list jobs", barethoven.APIVersion2, "")
			if err != nil {
				t.Logf("Error seen: %s", err)
				t.FailNow()
			}
			if stmt == nil {
				t.Log("stmt should not be nil")
				t.FailNow()
			} else if s, ok := stmt.(barethoven.Signal); ok {
				t.Logf("Expected to see Message, saw Signal: %s", s)
				t.FailNow()
			} else if m, ok := stmt.(barethoven.Message); ok {
				if m == nil || len(m) == 0 {
					t.Log("stmt is a Message, but it is empty")
					t.FailNow()
				}
			} else {
				t.Logf("Expected to see Statement of type Signal or Message, saw \"%s\"", reflect.TypeOf(stmt))
				t.FailNow()
			}
		})
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		sock, err := newTestSocket(t, defaultCatalogConfig)
		if err != nil {
			t.Logf("Error seen while creating client: %s", err)
			t.FailNow()
		}
		defer sock.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stmt, err := sock.Send(ctx, "list jobs", barethoven.APIVersion0, "")
		if err == nil {
			t.Log("Expected error, saw none")
			t.FailNow()
		}
		if stmt == nil {
			t.Log("stmt should not be nil")
			t.FailNow()
		} else if s, ok := stmt.(barethoven.Signal); ok {
			if s != barethoven.SignalCommandFailed {
				t.Logf("Expected to see Signal of type CommandFailed, saw \"%s\"", s)
				t.FailNow()
			}
		} else if m, ok := stmt.(barethoven.Message); ok {
			t.Logf("Expected stmt to be Signal, saw Message: %s", m)
			t.FailNow()
		} else {
			t.Logf("expected stmt to be Signal or Message, saw \"%s\"", reflect.TypeOf(stmt))
		}
		t.Run("SocketClosedAfterCancellation", func(t *testing.T) {
			_, err = sock.Send(context.Background(), "whatever", barethoven.APIVersion0, "")
			if err == nil {
				t.Log("Expected to see error when trying to send command after socket closed")
				t.FailNow()
			}
		})
	})

	t.Run("InvalidCommandFailsGracefully", func(t *testing.T) {
		t.Run("APIVersion0", func(t *testing.T) {
			sock, err := newTestSocket(t, defaultCatalogConfig)
			if err != nil {
				t.Logf("Error seen while creating client: %s", err)
				t.FailNow()
			}
			defer sock.Close()

			// NOTE: APIVersion0 does not seem to send an "InvalidCommand" signal in some / all cases
			// have to do actual response message checking...
			stmt, err := sock.Send(context.Background(), "configure add profile catalogacl=MyCatalog name=api-profile 2", barethoven.APIVersion0, "")
			if err != nil {
				t.Logf("APIVersion0 expected no error, saw: %s", err)
				t.FailNow()
			}
			if stmt == nil {
				t.Log("stmt should not be nil")
				t.FailNow()
			} else if s, ok := stmt.(barethoven.Signal); ok {
				t.Logf("Expected Message, saw Signal: %s", s)
				t.FailNow()
			} else if m, ok := stmt.(barethoven.Message); ok {
				if m == nil || len(m) == 0 {
					t.Log("stmt is a Message, but it is empty")
					t.FailNow()
				} else if !bytes.Contains(m, []byte("Missing value for directive")) {
					t.Logf("Expected to see \"Missing value for directive\" in message: \"%s\"", m)
					t.FailNow()
				}
			} else {
				t.Logf("Expected stmt to be Signal or Message, saw \"%s\"", reflect.TypeOf(stmt))
			}
		})
		t.Run("APIVersion1", func(t *testing.T) {
			sock, err := newTestSocket(t, defaultCatalogConfig)
			if err != nil {
				t.Logf("Error seen while creating client: %s", err)
				t.FailNow()
			}
			defer sock.Close()

			stmt, err := sock.Send(context.Background(), "configure add profile catalogacl=MyCatalog name=api-profile 2", barethoven.APIVersion1, "")
			if err == nil {
				t.Log("Expected to see error")
				t.FailNow()
			} else if te, ok := err.(*barethoven.CommandInvalidError); ok {
				t.Logf("Expected error to be CommandInvalid, saw \"%s\"", te)
			}
			if stmt == nil {
				t.Log("stmt should not be nil")
				t.FailNow()
			} else if s, ok := stmt.(barethoven.Signal); ok {
				t.Logf("Expected Message, saw Signal: %s", s)
				t.FailNow()
			} else if m, ok := stmt.(barethoven.Message); ok {
				if m == nil || len(m) == 0 {
					t.Log("stmt is a Message, but it is empty")
					t.FailNow()
				} else if !bytes.Contains(m, []byte("Missing value for directive")) {
					t.Logf("Expected to see \"Missing value for directive\" in message: \"%s\"", m)
					t.FailNow()
				}
			} else {
				t.Logf("Expected stmt to be Signal or Message, saw \"%s\"", reflect.TypeOf(stmt))
			}
		})
		t.Run("APIVersion2", func(t *testing.T) {
			sock, err := newTestSocket(t, defaultCatalogConfig)
			if err != nil {
				t.Logf("Error seen while creating client: %s", err)
				t.FailNow()
			}
			defer sock.Close()

			stmt, err := sock.Send(context.Background(), "configure add profile catalogacl=MyCatalog name=api-profile 2", barethoven.APIVersion2, "")
			if err == nil {
				t.Log("Expected to see error")
				t.FailNow()
			}
			if stmt == nil {
				t.Log("stmt should not be nil")
				t.FailNow()
			} else if s, ok := stmt.(barethoven.Signal); ok {
				t.Logf("Expected Message, saw Signal: %s", s)
				t.FailNow()
			} else if m, ok := stmt.(barethoven.Message); ok {
				if m == nil || len(m) == 0 {
					t.Log("stmt is a Message, but it is empty")
					t.FailNow()
				} else if !bytes.Contains(m, []byte("Missing value for directive")) {
					t.Logf("Expected to see \"Missing value for directive\" in message: \"%s\"", m)
					t.FailNow()
				}
			} else {
				t.Logf("Expected stmt to be Signal or Message, saw \"%s\"", reflect.TypeOf(stmt))
			}
		})
	})
}

func TestDefaultSocket_Converse(t *testing.T) {
	t.Run("ValidCommandsSucceed", func(t *testing.T) {
		t.Run("APIVersion0", func(t *testing.T) {
			sock, err := newTestSocket(t, defaultCatalogConfig)
			if err != nil {
				t.Logf("Error seen while creating client: %s", err)
				t.FailNow()
			}
			defer sock.Close()

			cmd, err := barethoven.NewCommand(context.Background(), "list jobs", nil)
			if err != nil {
				t.Logf("Error creating command: %s", err)
				t.FailNow()
			}
			err = sock.SendCommand(cmd)
			if err != nil {
				t.Logf("Could not converse: %s", err)
				t.FailNow()
			}
			waitForConclusion(t, cmd)
		})

		t.Run("APIVersion1", func(t *testing.T) {
			sock, err := newTestSocket(t, defaultCatalogConfig)
			if err != nil {
				t.Logf("Error seen while creating client: %s", err)
				t.FailNow()
			}
			defer sock.Close()

			cmd, err := barethoven.NewCommand(context.Background(), "list jobs", newDefaultCommandConfig(barethoven.APIVersion1))
			if err != nil {
				t.Logf("Error creating command: %s", err)
				t.FailNow()
			}
			err = sock.SendCommand(cmd)
			if err != nil {
				t.Logf("Could not converse: %s", err)
				t.FailNow()
			}
			waitForConclusion(t, cmd)
		})

		t.Run("APIVersion2", func(t *testing.T) {
			sock, err := newTestSocket(t, defaultCatalogConfig)
			if err != nil {
				t.Logf("Error seen while creating client: %s", err)
				t.FailNow()
			}
			defer sock.Close()

			cmd, err := barethoven.NewCommand(context.Background(), "list jobs", newDefaultCommandConfig(barethoven.APIVersion2))
			if err != nil {
				t.Logf("Error creating command: %s", err)
				t.FailNow()
			}
			err = sock.SendCommand(cmd)
			if err != nil {
				t.Logf("Could not converse: %s", err)
				t.FailNow()
			}
			waitForConclusion(t, cmd)
		})
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		sock, err := newTestSocket(t, defaultCatalogConfig)
		if err != nil {
			t.Logf("Error seen while creating client: %s", err)
			t.FailNow()
		}
		defer sock.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		cmd, err := barethoven.NewCommand(ctx, "list jobs", newDefaultCommandConfig(barethoven.APIVersion2))
		if err != nil {
			t.Logf("Error creating command: %s", err)
			t.FailNow()
		}
		err = sock.SendCommand(cmd)
		if err != nil {
			t.Logf("Could not converse: %s", err)
			t.FailNow()
		}
		var msg barethoven.Conclusion
		for d := range cmd.Spoke() {
			if d.DialogType() == barethoven.DialogConclusion {
				msg = d.(barethoven.Conclusion)
			}
		}

		if err := msg.Err(); err == nil {
			t.Logf("Expected to receive error about context cancellation, saw msg: %s", msg)
			t.FailNow()
		}
		t.Run("SocketClosedAfterCancellation", func(t *testing.T) {
			cmd, err := barethoven.NewCommand(context.Background(), "whatever", nil)
			if err != nil {
				t.Logf("Error creating command: %s", err)
				t.FailNow()
			}
			err = sock.SendCommand(cmd)
			if err == nil {
				t.Log("Expected to see error when trying to send command after socket closed")
				t.FailNow()
			}
		})
		t.Run("CanReopenSocket", func(t *testing.T) {
			err := sock.Open()
			if err != nil {
				t.Logf("Unable to re-open socket: %s", err)
				t.FailNow()
			}
		})
	})
}

func TestDefaultSocket_Heartbeat(t *testing.T) {
	sock, err := newTestSocket(t, &barethoven.SocketConfig{
		DefaultCatalog:    defaultCatalog,
		HeartbeatInterval: 1 * time.Second,
	})
	if err != nil {
		t.Logf("Error seen while creating socket: %s", err)
		t.FailNow()
	}

	time.Sleep(10 * time.Second)

	if sock.Closed() {
		t.Log("Socket should still be open at end of heartbeat test but it isn't, something broke...")
		t.FailNow()
	}

	sock.Close()
}

func TestDefaultSocket_SQL(t *testing.T) {
	t.Run("ValidSQLWorks", func(t *testing.T) {
		sock, err := newTestSocket(t, defaultCatalogConfig)
		if err != nil {
			t.Logf("Error seen while creating socket: %s", err)
			t.FailNow()
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd, err := barethoven.NewCommand(ctx, "sqlquery", barethoven.NewCommandConfig("", 0, 5*time.Second, "", nil))
		if err != nil {
			t.Logf("Error creating command: %s", err)
			t.FailNow()
		}
		err = sock.SendCommand(cmd)
		if err != nil {
			t.Logf("Unexpected error: %s", err)
			t.FailNow()
		}

		result := make([]byte, 0)
		attempted := false
		success := false
	Breakme:
		for {
			select {
			case <-ctx.Done():
				t.Logf("Context finished: %s", ctx.Err())
				t.FailNow()
				break Breakme
			case d, ok := <-cmd.Spoke():
				if !ok {
					break Breakme
				}
				if s, ok := d.(barethoven.Signal); ok {
					switch s {
					case barethoven.SignalSubPrompt:
						if attempted && len(result) > 0 {
							success = true
						}
					}
				} else if m, ok := d.(barethoven.Message); ok {
					if attempted {
						result = append(result, m...)
					}
				} else if q, ok := d.(*barethoven.DefaultQuestion); ok {
					if attempted {
						q.Respond(barethoven.SignalTerminate)
					} else {
						attempted = true
						q.Respond(barethoven.Message("select * from Job;"))
					}
				} else if conc, ok := d.(*barethoven.DefaultConclusion); ok {
					log.Printf("Conclusion: %v", conc)
					break Breakme
				}
			}
		}

		if success {
			log.Printf("SQL Query result: %s", string(result))
		} else {
			t.Log("Received subsequent SubPrompt signal without receiving any data from initial query, something went wrong")
			t.FailNow()
		}

		cancel()
		sock.Close()
	})

	return
}
