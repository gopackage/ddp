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

	// session contains the DDP session token (can be used for reconnects and debugging).
	session string
	// version contains the negotiated DDP protocol version in use.
	version string
	// ws is the underlying websocket being used.
	ws *websocket.Conn
	// inbox is an incoming message channel
	inbox chan map[string]interface{}
	// errors is an incoming errors channel
	errors chan error
	// pingTimer is a timer for sending regular pings to the server
	pingTimer *time.Timer
	// pings tracks inflight pings based on each ping ID.
	pings map[string][]*pingTracker
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
	return NewClientWithWebsocket(ws)
}

// NewClientWithWebsocket creates a Client with an existing websocket.
func NewClientWithWebsocket(ws *websocket.Conn) (*Client, error) {
	client := defaultClient(ws)
	// We spin off an inbox stuffing goroutine
	go inboxWorker(ws, client.inbox, client.errors, client.pingTimer, client.HeartbeatInterval)

	// Start DDP connection
	// Is there any merit to making this a struct and using json.Marshal?
	ws.Write([]byte("{\"msg\":\"connect\",\"version\":\"1\",\"support\":[\"1\"]}"))
	// Read until we get a response - it will either be `connected` or `failed`
	// The server may send a message without the `msg` key (which should be ignored).
	for {
		select {
		case msg := <-client.inbox:
			// Message!
			mtype, ok := msg["msg"]
			if ok {
				switch mtype.(string) {
				case "connected":
					client.version = "1" // Currently the only version we support
					client.session = msg["session"].(string)
					// Start automatic heartbeats
					client.pingTimer = time.AfterFunc(client.HeartbeatInterval, func() {
						client.Ping()
						client.pingTimer.Reset(client.HeartbeatInterval)
					})
					// Start manager
					go inboxManager(client)
					return client, nil
				case "failed":
					return nil, fmt.Errorf("Failed to connect, we support version 1 but server supports %s", msg["version"])
				default:
					// Ignore?
					log.Println("Unexpected connection message", msg)
				}
			}
		case err := <-client.errors:
			client.ws.Close()
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
func (c *Client) Reconnect() {
	// TODO implement reconnect
	// Shutdown out all outstanding pings
	// Shutdown inbox worker
	// Close websocket
	// Reconnect
}

// Ping sends a heartbeat signal to the server. The Ping doesn't look for
// a response but may trigger the connection to reconnect if the ping timesout.
// This is primarily useful for reviving an unresponsive Client connection.
func (c *Client) Ping() {
	c.PingPong("", c.HeartbeatTimeout, func(err error) {
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
	var msg []byte
	if len(id) == 0 {
		msg = []byte("{\"msg\":\"ping\"}")
	} else {
		msg = []byte("{\"msg\":\"ping\",\"id\":\"" + id + "\"}")
	}
	_, err := c.ws.Write(msg)
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

// Close implements the io.Closer interface.
func (c *Client) Close() {
	c.ws.Close()
}

type pingTracker struct {
	handler func(error)
	timeout time.Duration
	timer   *time.Timer
}

// defaultClient creates a default Client.
func defaultClient(ws *websocket.Conn) *Client {
	return &Client{
		HeartbeatInterval: 30 * time.Second, // Meteor impl default - 5 (we ping first)
		HeartbeatTimeout:  15 * time.Second, // Meteor impl default
		ws:                ws,
		inbox:             make(chan map[string]interface{}, 100),
		errors:            make(chan error, 100),
		pings:             map[string][]*pingTracker{},
	}
}

// inboxManager pulls messages from the inbox and routes them to appropriate
// handlers.
func inboxManager(client *Client) {
	for {
		select {
		case msg := <-client.inbox:
			// Message!
			mtype, ok := msg["msg"]
			if ok {
				switch mtype.(string) {

				// Heartbeats
				case "ping":
					// We received a ping - need to respond with a pong
					id, ok := msg["id"]
					if ok {
						client.ws.Write([]byte("{\"msg\":\"pong\",\"id\":" + id.(string) + "\"}"))
					} else {
						client.ws.Write([]byte("{\"msg\":\"pong\"}"))
					}
				case "pong":
					// We received a pong - we can clear the ping tracker and call its handler
					id, ok := msg["id"]
					var key string
					if ok {
						key = id.(string)
					}
					pings, ok := client.pings[key]
					if ok && len(pings) > 0 {
						ping := pings[0]
						pings = pings[1:]
						if len(key) == 0 || len(pings) > 0 {
							client.pings[key] = pings
						}
						ping.timer.Stop()
						ping.handler(nil)
					}

				// Live Data
				case "nosub":
					log.Printf("Failed to connect, we support version 1 but server supports %s\n", msg["version"])
				case "added":
					log.Printf("Failed to connect, we support version 1 but server supports %s\n", msg["version"])
				case "changed":
					log.Printf("Failed to connect, we support version 1 but server supports %s\n", msg["version"])
				case "removed":
					log.Printf("Failed to connect, we support version 1 but server supports %s\n", msg["version"])
				case "ready":
					log.Printf("Failed to connect, we support version 1 but server supports %s\n", msg["version"])
				case "addedBefore":
					log.Printf("Failed to connect, we support version 1 but server supports %s\n", msg["version"])
				case "movedBefore":
					log.Printf("Failed to connect, we support version 1 but server supports %s\n", msg["version"])

				// RPC
				case "result":
				case "updated":
				default:
					// Ignore?
					log.Println("Unexpected message", msg)
				}
			}
		case err := <-client.errors:
			log.Println("Error", err)
		}
	}
}

// inboxWorker pulls messages from a websocket, decodes JSON packets, and
// stuffs them into a message channel.
func inboxWorker(ws io.Reader, inbox chan<- map[string]interface{}, errors chan<- error, timer *time.Timer, interval time.Duration) {
	// Read until we get a response - it will either be `connected` or `failed`
	// The server may send a message without the `msg` key (which should be ignored).
	buffer := make([]byte, 4096)
	var read int
	var err error
	for {
		// TODO this needs to move to a streaming decoder - messages can be
		// pretty big!
		for read, err = ws.Read(buffer); read == len(buffer) || err != nil; read, err = ws.Read(buffer) {
			// Buffer not big enough - we read until drained
			if read == 0 {
				// This can loop infinitely fast with read == 0 so we will
				// sleep so we don't use up all the available CPU.
				log.Println("ddp.start ######### ws timeout")
				time.Sleep(1 * time.Second)
			} else {
				log.Println("ddp.start reading event", read)
			}
			if timer != nil {
				timer.Reset(interval)
			}
		}
		if timer != nil {
			timer.Reset(interval)
		}
		var event interface{}
		err = json.Unmarshal(buffer[0:read], &event)
		if err != nil {
			errors <- err
		} else {
			inbox <- event.(map[string]interface{})
		}
	}
}
