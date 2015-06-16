// Package ddp implements the MeteorJS DDP protocol over websockets. Fallback
// to longpolling is NOT supported (and is not planned on ever being supported
// by this library). We will try to model the library after `net/http` - right
// now the library is barebones and doesn't provide the pluggability of http.
// However, that's the goal for the package eventually.
package ddp

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

// debugLog is true if we should log debugging information about the connection
var debugLog = true

// Client represents a DDP client connection. The DDP client establish a DDP
// session and acts as a message pump for other tools.
type Client struct {
	// HeartbeatInterval is the time between heartbeats to send
	HeartbeatInterval time.Duration
	// HeartbeatTimeout is the time for a heartbeat ping to timeout
	HeartbeatTimeout time.Duration
	// ReconnectInterval is the time between reconnections on bad connections
	ReconnectInterval time.Duration

	// session contains the DDP session token (can be used for reconnects and debugging).
	session string
	// version contains the negotiated DDP protocol version in use.
	version string
	// ws is the underlying websocket being used.
	ws *websocket.Conn
	// encoder is a JSON encoder to send outgoing packets to the websocket.
	encoder *json.Encoder
	// url the URL the websocket is connected to
	url string
	// origin is the origin for the websocket connection
	origin string
	// inbox is an incoming message channel
	inbox chan map[string]interface{}
	// errors is an incoming errors channel
	errors chan error
	// inboxDone is a channel to control the inbox worker
	inboxDone chan bool
	// pingTimer is a timer for sending regular pings to the server
	pingTimer *time.Timer
	// pings tracks inflight pings based on each ping ID.
	pings map[string][]*pingTracker
	// calls tracks method invocations that are still in flight
	calls map[string]*Call
	// nextID is the next ID for API calls
	nextID uint64
	// idMutex is a mutex to protect ID updates
	idMutex *sync.Mutex
}

// NewClient creates a default client (using an internal websocket) to the
// provided URL using the origin for the connection. The client will
// automatically connect, upgrade to a websocket, and establish a DDP
// connection session before returning the client. The client will
// automatically and internally handle heartbeats and reconnects.
//
// TBD create an option to use an external websocket (aka htt.Transport)
// TBD create an option to substitute heartbeat and reconnect behavior (aka http.Tranport)
// TBD create an option to hijack the connection (aka http.Hijacker)
// TBD create profiling features (aka net/http/pprof)
func NewClient(url, origin string) (*Client, error) {
	ws, err := websocket.Dial(url, "", origin)
	if err != nil {
		return nil, err
	}
	c := defaultClient(ws, url, origin)
	// We spin off an inbox stuffing goroutine
	go c.inboxWorker()

	// Start DDP connection
	c.Send(NewConnect())
	// Read until we get a response - it will either be `connected` or `failed`
	// The server may send a message without the `msg` key (which should be ignored).
	for {
		select {
		case msg := <-c.inbox:
			// Message!
			mtype, ok := msg["msg"]
			if ok {
				switch mtype.(string) {
				case "connected":
					c.version = "1" // Currently the only version we support
					c.session = msg["session"].(string)
					// Start automatic heartbeats
					c.pingTimer = time.AfterFunc(c.HeartbeatInterval, func() {
						c.Ping()
						c.pingTimer.Reset(c.HeartbeatInterval)
					})
					// Start manager
					go c.inboxManager()
					return c, nil
				case "failed":
					return nil, fmt.Errorf("Failed to connect, we support version 1 but server supports %s", msg["version"])
				default:
					// Ignore?
					log.Println("Unexpected connection message", msg)
				}
			}
		case err := <-c.errors:
			c.ws.Close()
			return nil, err
		}
	}
}

// Session returns the negotiated session token for the connection.
func (c *Client) Session() string {
	return c.session
}

// Version returns the negotiated protocol version in use by the client.
func (c *Client) Version() string {
	return c.version
}

// Reconnect attempts to reconnect the client to the server on the existing
// DDP session.
//
// TODO reconnect should also track and resend any subscriptions that are
// active and any methods that may have been in flight as the resumed session
// does not resume subscriptions and methods may or may not have been sent
// and are supposed to be idempotent
func (c *Client) Reconnect() {
	log.Println("Reconnecting...")
	// Shutdown out all outstanding pings
	c.pingTimer.Stop()
	// Close websocket
	c.ws.Close()
	// Stop inbox manager
	c.inboxDone <- true
	// Reconnect
	ws, err := websocket.Dial(c.url, "", c.origin)
	if err != nil {
		log.Println("Dial error", err)
		// Reconnect again after set interval
		time.AfterFunc(c.ReconnectInterval, c.Reconnect)
		return
	}
	log.Println("Dialed")

	c.ws = ws
	// We spin off an inbox stuffing goroutine
	go c.inboxWorker()

	// Start DDP connection
	c.Send(NewReconnect(c.session))

	log.Println("Attempted to resume session")

	// Read until we get a response - it will either be `connected` or `failed`
	// The server may send a message without the `msg` key (which should be ignored).
	for {
		select {
		case msg := <-c.inbox:
			// Message!
			mtype, ok := msg["msg"]
			if ok {
				switch mtype.(string) {
				case "connected":
					c.version = "1" // Currently the only version we support
					c.session = msg["session"].(string)
					// Start automatic heartbeats
					c.pingTimer = time.AfterFunc(c.HeartbeatInterval, func() {
						c.Ping()
						c.pingTimer.Reset(c.HeartbeatInterval)
					})
					// Start manager
					go c.inboxManager()
					log.Println("Reconnected")
					return
				case "failed":
					log.Printf("Failed to connect, we support version 1 but server supports %s", msg["version"])
					// Reconnect again after set interval
					time.AfterFunc(c.ReconnectInterval, c.Reconnect)
					return
				default:
					// Ignore?
					log.Println("Unexpected connection message", msg)
				}
			}
		case err := <-c.errors:
			c.ws.Close()
			log.Println("Reconnection error", err)
			// Reconnect again after set interval
			time.AfterFunc(c.ReconnectInterval, c.Reconnect)
			return
		}
	}
}

// Call represents an active RPC.
type Call struct {
	ID            string      // The uuid for this method call
	ServiceMethod string      // The name of the service and method to call.
	Args          interface{} // The argument to the function (*struct).
	Reply         interface{} // The reply from the function (*struct).
	Error         error       // After completion, the error status.
	Done          chan *Call  // Strobes when call is complete.
	Owner         *Client     // Client that owns the method call
}

func (call *Call) done() {
	delete(call.Owner.calls, call.ID)
	select {
	case call.Done <- call:
		// ok
	default:
		// We don't want to block here.  It is the caller's responsibility to make
		// sure the channel has enough buffer space. See comment in Go().
		if debugLog {
			log.Println("rpc: discarding Call reply due to insufficient Done chan capacity")
		}
	}
}

// Go invokes the function asynchronously.  It returns the Call structure representing
// the invocation.  The done channel will signal when the call is complete by returning
// the same Call object.  If done is nil, Go will allocate a new channel.
// If non-nil, done must be buffered or Go will deliberately crash.
//
// Go and Call are modeled after the standard `net/rpc` package versions.
func (c *Client) Go(serviceMethod string, args []interface{}, reply interface{}, done chan *Call) *Call {

	call := new(Call)
	call.ID = c.newID()
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Reply = reply
	if done == nil {
		done = make(chan *Call, 10) // buffered.
	} else {
		// If caller passes done != nil, it must arrange that
		// done has enough buffer for the number of simultaneous
		// RPCs that will be using that channel.  If the channel
		// is totally unbuffered, it's best not to run at all.
		if cap(done) == 0 {
			log.Panic("ddp.rpc: done channel is unbuffered")
		}
	}
	call.Done = done
	c.calls[call.ID] = call

	//c.ws.Write(call)

	return call
}

// Call invokes the named function, waits for it to complete, and returns its error status.
func (c *Client) Call(serviceMethod string, args []interface{}, reply interface{}) error {
	call := <-c.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}

// Ping sends a heartbeat signal to the server. The Ping doesn't look for
// a response but may trigger the connection to reconnect if the ping timesout.
// This is primarily useful for reviving an unresponsive Client connection.
func (c *Client) Ping() {
	c.PingPong(c.newID(), c.HeartbeatTimeout, func(err error) {
		if err != nil {
			// Is there anything else we should or can do?
			c.Reconnect()
		}
	})
}

// PingPong sends a heartbeat signal to the server and calls the provided
// function when a pong is received. An optional id can be sent to help
// track the responses - or an empty string can be used. It is the
// responsibility of the caller to respond to any errors that may occur.
func (c *Client) PingPong(id string, timeout time.Duration, handler func(error)) {
	_, err := c.Send(NewPing(id))
	if err != nil {
		handler(err)
		return
	}
	pings, ok := c.pings[id]
	if !ok {
		pings = make([]*pingTracker, 0, 5)
	}
	tracker := &pingTracker{handler: handler, timeout: timeout, timer: time.AfterFunc(timeout, func() {
		handler(fmt.Errorf("ping timeout"))
	})}
	c.pings[id] = append(pings, tracker)
}

// Send transmits messages to the server.
func (c *Client) Send(msg interface{}) (int, error) {
	switch msg.(type) {
	case []byte:
		return c.ws.Write(msg.([]byte))
	default:
		return -1, c.encoder.Encode(msg)
	}
}

// Close implements the io.Closer interface.
func (c *Client) Close() {
	c.ws.Close()
}

type pingTracker struct {
	handler func(error)
	timeout time.Duration
	timer   *time.Timer
}

// newID issues a new ID for use in calls.
func (c *Client) newID() string {
	c.idMutex.Lock()
	next := c.nextID
	c.nextID++
	c.idMutex.Unlock()
	return fmt.Sprintf("%x", next)
}

// defaultClient creates a default Client.
func defaultClient(ws *websocket.Conn, url, origin string) *Client {
	c := &Client{
		HeartbeatInterval: 30 * time.Second, // Meteor impl default - 5 (we ping first)
		HeartbeatTimeout:  15 * time.Second, // Meteor impl default
		ReconnectInterval: 5 * time.Second,
		ws:                ws,
		url:               url,
		origin:            origin,
		inbox:             make(chan map[string]interface{}, 100),
		errors:            make(chan error, 100),
		inboxDone:         make(chan bool, 0),
		pings:             map[string][]*pingTracker{},
		idMutex:           new(sync.Mutex),
	}
	c.encoder = json.NewEncoder(ws)
	return c
}

// inboxManager pulls messages from the inbox and routes them to appropriate
// handlers.
func (c *Client) inboxManager() {
	for {
		select {
		case msg := <-c.inbox:
			// Message!
			mtype, ok := msg["msg"]
			if ok {
				switch mtype.(string) {

				// Heartbeats
				case "ping":
					// We received a ping - need to respond with a pong
					id, ok := msg["id"]
					if ok {
						c.Send(NewPong(id.(string)))
					} else {
						c.Send(NewPong(""))
					}
				case "pong":
					// We received a pong - we can clear the ping tracker and call its handler
					id, ok := msg["id"]
					var key string
					if ok {
						key = id.(string)
					}
					pings, ok := c.pings[key]
					if ok && len(pings) > 0 {
						ping := pings[0]
						pings = pings[1:]
						if len(key) == 0 || len(pings) > 0 {
							c.pings[key] = pings
						}
						ping.timer.Stop()
						ping.handler(nil)
					}

				// Live Data
				case "nosub":
					log.Println(mtype, msg)
				case "added":
					log.Println(mtype, msg)
				case "changed":
					log.Println(mtype, msg)
				case "removed":
					log.Println(mtype, msg)
				case "ready":
					log.Println(mtype, msg)
				case "addedBefore":
					log.Println(mtype, msg)
				case "movedBefore":
					log.Println(mtype, msg)

				// RPC
				case "result":
					log.Println(mtype, msg)
				case "updated":
					log.Println(mtype, msg)

				default:
					// Ignore?
					log.Println("Unexpected message", msg)
				}
			}
		case err := <-c.errors:
			log.Println("Error", err)
		case _ = <-c.inboxDone:
			log.Println("Inbox finished processing")
			break
		}
	}
}

// inboxWorker pulls messages from a websocket, decodes JSON packets, and
// stuffs them into a message channel.
func (c *Client) inboxWorker() {
	dec := json.NewDecoder(c.ws)
	for {
		var event interface{}

		if err := dec.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			c.errors <- err
		}
		if c.pingTimer != nil {
			c.pingTimer.Reset(c.HeartbeatInterval)
		}
		c.inbox <- event.(map[string]interface{})
	}
	// Spawn a reconnect
	c.Reconnect()
}
