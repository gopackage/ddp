package main

import (
	"encoding/json"
	"time"

	"github.com/apex/log"
	"github.com/gopackage/ddp"
)

const (
	user = "meteorite"
	pass = "password"
)

func main() {

	// Turn up logging
	log.SetLevel(log.DebugLevel)

	// Assumes Meteor running in development mode normally (no custom port etc.).
	client := ddp.NewClient("ws://localhost:3000/websocket", "http://localhost/")
	defer client.Close()

	// Connect to the server
	err := client.Connect()
	if err != nil {
		log.WithError(err).Fatal("could not connect")
	}
	log.Debug("connected")

	// Login our user - Meteor.loginWithPassword implements logins using the `login` method. The DDP library
	// provides the implementation for the data the method call expects in ddp.NewUsernameLogin and ddp.NewEmailLogin
	// respectively.
	login, err := client.Call("login", ddp.NewUsernameLogin(user, pass))
	if err != nil {
		log.WithError(err).Fatal("failed login")
	} else {
		log.WithField("response", login).Info("logged in")
	}

	// We send a parameter to the `tasks` subscription to demonstrate what that looks like but the tutorial doesn't
	// use this parameter.
	err = client.Sub("tasks", "abc")
	if err != nil {
		log.WithError(err).Fatal("could not subscribe")
	}

	// We know client.Sub is synchronous and will only respond after we connect.
	log.WithFields(log.Fields{"version": client.Version(), "session": client.Session()}).Info("ready")

	tasks := client.CollectionByName("tasks")
	log.WithField("count", len(tasks.FindAll())).Info("Collection: tasks")
	for key, value := range tasks.FindAll() {
		log.WithField("id", key).Info("task found")
		data, _ := json.Marshal(value)
		log.WithField("data", string(data)).Info("task")
	}

	time.Sleep(5 * time.Second)

	// Insert a task using a meteor method call
	log.Info("sending RPC method call to create a task")
	response, err := client.Call("tasks.insert", "hello " + time.Now().String())
	if err != nil {
		log.WithError(err).Fatal("task create failed")
	} else {
		log.WithField("response", response).Info("created task")
	}

	// Monitor activity over time. If you create/remove tasks via the Meteor web UI, you will see the tasks collection
	// size change to match.
	for {
		log.WithField("stats", client.Stats()).Info("update")
		log.WithField("count", len(tasks.FindAll())).Info("Collection: tasks")

		time.Sleep(10 * time.Second)
	}
}
