package ddp

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"golang.org/x/net/websocket"
)

// Client represents a DDP client connection. The DDP client establish a DDP
// session and acts as a message pump for other tools.
type Client struct {
	// HeartbeatInterval is the time between heartbeats to send
	HeartbeatInterval time.Duration
	// HeartbeatTimeout is the time for a heartbeat ping to timeout
	HeartbeatTimeout time.Duration
	// ReconnectInterval is the time between reconnections on bad connections
	ReconnectInterval time.Duration
	// Collections contains all the collections currently subscribed
	Collections map[string]Collection

	// writeStats controls statistics gathering for current websocket writes.
	writeSocketStats *WriterStats
	// writeStats controls statistics gathering for overall client writes.
	writeStats *WriterStats
	// writeLog controls logging for client writes.
	writeLog *WriterLogger
	// readStats controls statistics gathering for current websocket reads.
	readSocketStats *ReaderStats
	// readStats controls statistics gathering for overall client reads.
	readStats *ReaderStats
	// readLog control logging for clietn reads.
	readLog *ReaderLogger
	// reconnects in the number of reconnections the client has made
	reconnects int64
	// pingsIn is the number of pings received from the server
	pingsIn int64
	// pingsOut is te number of pings sent by the client
	pingsOut int64

	// session contains the DDP session token (can be used for reconnects and debugging).
	session string
	// version contains the negotiated DDP protocol version in use.
	version string
	// serverID the cluster node ID for the server we connected to
	serverID string
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
	// pingTimer is a timer for sending regular pings to the server
	pingTimer *time.Timer
	// pings tracks inflight pings based on each ping ID.
	pings map[string][]*pingTracker
	// calls tracks method invocations that are still in flight
	calls map[string]*Call
	// subs tracks active subscriptions. Map contains name->args
	subs map[string]*Call

	// idManager tracks IDs for ddp messages
	idManager
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
	c := &Client{
		HeartbeatInterval: 45 * time.Second, // Meteor impl default + 10 (we ping last)
		HeartbeatTimeout:  15 * time.Second, // Meteor impl default
		ReconnectInterval: 5 * time.Second,
		Collections:       map[string]Collection{},
		ws:                ws,
		url:               url,
		origin:            origin,
		inbox:             make(chan map[string]interface{}, 100),
		errors:            make(chan error, 100),
		pings:             map[string][]*pingTracker{},
		calls:             map[string]*Call{},
		subs:              map[string]*Call{},

		// Stats
		writeSocketStats: NewWriterStats(nil),
		writeStats:       NewWriterStats(nil),
		readSocketStats:  NewReaderStats(nil),
		readStats:        NewReaderStats(nil),

		// Loggers
		writeLog: NewWriterTextLogger(nil),
		readLog:  NewReaderTextLogger(nil),

		idManager: *newidManager(),
	}
	c.encoder = json.NewEncoder(c.writeStats)
	c.SetSocketLogActive(false)

	// We spin off an inbox processing goroutine
	go c.inboxManager()

	// Start DDP connection
	c.start(ws, NewConnect())

	return c, nil
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
// TODO needs a reconnect backoff so we don't trash a down server
// TODO reconnect should not allow more reconnects while a reconnection is already in progress.
func (c *Client) Reconnect() {

	c.Close()

	c.reconnects++

	// Reconnect
	ws, err := websocket.Dial(c.url, "", c.origin)
	if err != nil {
		log.Println("Dial error", err)
		// Reconnect again after set interval
		time.AfterFunc(c.ReconnectInterval, c.Reconnect)
		return
	}

	c.start(ws, NewReconnect(c.session))

	// --------------------------------------------------------------------
	// We resume inflight or ongoing subscriptions - we don't have to wait
	// for connection confirmation (messages can be pipelined).
	// --------------------------------------------------------------------

	// Send calls that haven't been confirmed - may not have been sent
	// and effects should be idempotent
	for _, call := range c.calls {
		log.Println("resending inflight method", call.ServiceMethod)
		c.Send(NewMethod(call.ID, call.ServiceMethod, call.Args.([]interface{})))
	}

	// Resend subscriptions and patch up collections
	for _, sub := range c.subs {
		log.Println("restarting active subscription", sub.ServiceMethod)
		c.Send(NewSub(sub.ID, sub.ServiceMethod, sub.Args.([]interface{})))
	}
	// Patching up the collections right now is just resetting them. There
	// must be a better way but this is quick and works.
	c.Collections = map[string]Collection{}
}

// Subscribe subscribes to data updates.
func (c *Client) Subscribe(subName string, args []interface{}, done chan *Call) *Call {
	call := new(Call)
	call.ID = c.newID()
	call.ServiceMethod = subName
	call.Args = args
	call.Owner = c
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
	c.subs[call.ID] = call

	// Save this subscription to the client so we can reconnect
	subArgs := make([]interface{}, len(args))
	copy(subArgs, args)

	c.Send(NewSub(call.ID, subName, args))

	return call
}

// Sub sends a synchronous subscription request to the server.
func (c *Client) Sub(subName string, args []interface{}) error {
	call := <-c.Subscribe(subName, args, make(chan *Call, 1)).Done
	return call.Error
}

// Go invokes the function asynchronously.  It returns the Call structure representing
// the invocation.  The done channel will signal when the call is complete by returning
// the same Call object.  If done is nil, Go will allocate a new channel.
// If non-nil, done must be buffered or Go will deliberately crash.
//
// Go and Call are modeled after the standard `net/rpc` package versions.
func (c *Client) Go(serviceMethod string, args []interface{}, done chan *Call) *Call {

	call := new(Call)
	call.ID = c.newID()
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Owner = c
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

	c.Send(NewMethod(call.ID, serviceMethod, args))

	return call
}

// Call invokes the named function, waits for it to complete, and returns its error status.
func (c *Client) Call(serviceMethod string, args []interface{}) (interface{}, error) {
	call := <-c.Go(serviceMethod, args, make(chan *Call, 1)).Done
	return call.Reply, call.Error
}

// Ping sends a heartbeat signal to the server. The Ping doesn't look for
// a response but may trigger the connection to reconnect if the ping timesout.
// This is primarily useful for reviving an unresponsive Client connection.
func (c *Client) Ping() {
	c.PingPong(c.newID(), c.HeartbeatTimeout, func(err error) {
		if err != nil {
			// Is there anything else we should or can do?
			go c.Reconnect()
		}
	})
}

// PingPong sends a heartbeat signal to the server and calls the provided
// function when a pong is received. An optional id can be sent to help
// track the responses - or an empty string can be used. It is the
// responsibility of the caller to respond to any errors that may occur.
func (c *Client) PingPong(id string, timeout time.Duration, handler func(error)) {
	err := c.Send(NewPing(id))
	if err != nil {
		handler(err)
		return
	}
	c.pingsOut++
	pings, ok := c.pings[id]
	if !ok {
		pings = make([]*pingTracker, 0, 5)
	}
	tracker := &pingTracker{handler: handler, timeout: timeout, timer: time.AfterFunc(timeout, func() {
		handler(fmt.Errorf("ping timeout"))
	})}
	c.pings[id] = append(pings, tracker)
}

// Send transmits messages to the server. The msg parameter must be json
// encoder compatible.
func (c *Client) Send(msg interface{}) error {
	return c.encoder.Encode(msg)
}

// Close implements the io.Closer interface.
func (c *Client) Close() {
	// Shutdown out all outstanding pings
	c.pingTimer.Stop()
	// Close websocket
	c.ws.Close()
	c.ws = nil
}

// ResetStats resets the statistics for the client.
func (c *Client) ResetStats() {
	c.readSocketStats.Reset()
	c.readStats.Reset()
	c.writeSocketStats.Reset()
	c.writeStats.Reset()
	c.reconnects = 0
	c.pingsIn = 0
	c.pingsOut = 0
}

// Stats returns the read and write statistics of the client.
func (c *Client) Stats() *ClientStats {
	return &ClientStats{
		Reads:       c.readSocketStats.Snapshot(),
		TotalReads:  c.readStats.Snapshot(),
		Writes:      c.writeSocketStats.Snapshot(),
		TotalWrites: c.writeStats.Snapshot(),
		Reconnects:  c.reconnects,
		PingsSent:   c.pingsOut,
		PingsRecv:   c.pingsIn,
	}
}

// SocketLogActive returns the current logging status for the socket.
func (c *Client) SocketLogActive() bool {
	return c.writeLog.Active
}

// SetSocketLogActive to true to enable logging of raw socket data.
func (c *Client) SetSocketLogActive(active bool) {
	c.writeLog.Active = active
	c.readLog.Active = active
}

// ClientStats displays combined statistics for the Client.
type ClientStats struct {
	// Reads provides statistics on the raw i/o network reads for the current connection.
	Reads *Stats
	// Reads provides statistics on the raw i/o network reads for the all client connections.
	TotalReads *Stats
	// Writes provides statistics on the raw i/o network writes for the current connection.
	Writes *Stats
	// Writes provides statistics on the raw i/o network writes for all the client connections.
	TotalWrites *Stats
	// Reconnects is the number of reconnections the client has made.
	Reconnects int64
	// PingsSent is the number of pings sent by the client
	PingsSent int64
	// PingsRecv is the number of pings received by the client
	PingsRecv int64
}

// start starts a new client connection on the provided websocket
func (c *Client) start(ws *websocket.Conn, connect *Connect) {

	c.ws = ws
	c.writeLog.SetWriter(ws)
	c.writeSocketStats = NewWriterStats(c.writeLog)
	c.writeStats.SetWriter(c.writeSocketStats)
	c.readLog.SetReader(ws)
	c.readSocketStats = NewReaderStats(c.readLog)
	c.readStats.SetReader(c.readSocketStats)

	// We spin off an inbox stuffing goroutine
	go c.inboxWorker(c.readStats)

	c.Send(connect)
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

				// Connection management
				case "connected":
					c.version = "1" // Currently the only version we support
					c.session = msg["session"].(string)
					// Start automatic heartbeats
					c.pingTimer = time.AfterFunc(c.HeartbeatInterval, func() {
						c.Ping()
						c.pingTimer.Reset(c.HeartbeatInterval)
					})
				case "failed":
					log.Fatalf("IM Failed to connect, we support version 1 but server supports %s", msg["version"])

				// Heartbeats
				case "ping":
					// We received a ping - need to respond with a pong
					id, ok := msg["id"]
					if ok {
						c.Send(NewPong(id.(string)))
					} else {
						c.Send(NewPong(""))
					}
					c.pingsIn++
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
					log.Println("Subscription returned a nosub error", msg)
					// Clear related subscriptions=
					sub, ok := msg["id"]
					if ok {
						delete(c.subs, sub.(string))
					}
				case "ready":
					// Run 'done' callbacks on all ready subscriptions
					subs, ok := msg["subs"]
					if ok {
						for _, sub := range subs.([]interface{}) {
							call, ok := c.subs[sub.(string)]
							if ok {
								call.done()
							}
						}
					}
				case "added":
					c.collectionBy(msg).added(msg)
				case "changed":
					c.collectionBy(msg).changed(msg)
				case "removed":
					c.collectionBy(msg).removed(msg)
				case "addedBefore":
					c.collectionBy(msg).addedBefore(msg)
				case "movedBefore":
					c.collectionBy(msg).movedBefore(msg)

				// RPC
				case "result":
					id, ok := msg["id"]
					if ok {
						call := c.calls[id.(string)]
						delete(c.calls, id.(string))
						e, ok := msg["error"]
						if ok {
							call.Error = fmt.Errorf(e.(string))
						} else {
							call.Reply = msg["result"]
						}
						call.done()
					}
				case "updated":
					// We currently don't do anything with updated status

				default:
					// Ignore?
					log.Println("Server sent unexpected message", msg)
				}
			} else {
				// Current Meteor server sends an undocumented DDP message
				// (looks like clustering "hint"). We will register and
				// ignore rather than log an error.
				serverID, ok := msg["server_id"]
				if ok {
					switch ID := serverID.(type) {
					case string:
						c.serverID = ID
					default:
						log.Println("Server cluster node", serverID)
					}
				} else {
					log.Println("Server sent message with no `msg` field", msg)
				}
			}
		case err := <-c.errors:
			log.Println("Websocket error", err)
		}
	}
}

func (c *Client) collectionBy(msg map[string]interface{}) Collection {
	n, ok := msg["collection"]
	if !ok {
		return NewMockCollection()
	}
	var collection Collection
	switch name := n.(type) {
	case string:
		collection, ok = c.Collections[name]
		if !ok {
			collection = NewCollection(name)
			c.Collections[name] = collection
		}
	default:
		return NewMockCollection()
	}

	return collection
}

// inboxWorker pulls messages from a websocket, decodes JSON packets, and
// stuffs them into a message channel.
func (c *Client) inboxWorker(ws io.Reader) {
	dec := json.NewDecoder(ws)
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
		if event == nil {
			log.Println("Inbox worker found nil event. May be due to broken websocket. Reconnecting.")
			break
		} else {
			c.inbox <- event.(map[string]interface{})
		}
	}

	// Spawn a reconnect
	time.AfterFunc(3*time.Second, func() {
		c.Reconnect()
	})
}
