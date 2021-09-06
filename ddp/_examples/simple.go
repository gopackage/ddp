package main

import (
	"encoding/json"
	"time"

	"github.com/apex/log"
	"github.com/gopackage/ddp"
)

func main() {

	// Turn up logging
	log.SetLevel(log.DebugLevel)

	client := ddp.NewClient("ws://localhost:3000/websocket", "http://localhost/")
	defer client.Close()

	err := client.Connect()
	if err != nil {
		log.WithError(err).Fatal("could not connect")
	}
	log.Debug("connected")

	err = client.Sub("builds", []interface{}{"abc"})
	if err != nil {
		log.WithError(err).Fatal("could not subscribe")
	}

	// We know client.Sub is synchronous and will only respond after we connect.
	log.WithFields(log.Fields{"version": client.Version(), "session": client.Session()}).Info("ready")

	builds := client.CollectionByName("builds")
	log.WithField("count", len(builds.FindAll())).Info("Collection: builds")
	for key, value := range builds.FindAll() {
		log.WithField("id", key).Info("build found")
		data, _ := json.Marshal(value)
		log.WithField("data", string(data)).Info("build")
	}

	time.Sleep(5 * time.Second)
	log.Info("sending RPC method call to 'ping' service")
	response, err := client.Call("ping", []interface{}{"hello"})
	if err != nil {
		log.WithError(err).Fatal("ping failed")
	} else {
		log.WithField("response", response).Info("hello")
	}
	for {
		log.WithField("stats", client.Stats()).Info("update")
		log.WithField("count", len(builds.FindAll())).Info("Collection: builds")

		time.Sleep(10 * time.Second)
	}
}
