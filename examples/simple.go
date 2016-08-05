package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gopackage/ddp"
)

func main() {
	client := ddp.NewClient("ws://localhost:3000/websocket", "http://localhost/")
	defer client.Close()

	err := client.Sub("builds", []interface{}{"abc"})
	if err != nil {
		log.Fatalln(err)
	}

	// We know client.Sub is synchronous and will only respond after we connect.
	log.Printf("Connected DDP version: %s session: %s", client.Version(), client.Session())

	builds := client.CollectionByName("builds")
	log.Println("Collection: builds", len(builds.FindAll()))
	for key, value := range builds.FindAll() {
		log.Println(key)
		data, _ := json.Marshal(value)
		log.Println(string(data))
	}

	time.Sleep(5 * time.Second)
	log.Printf("Sending RPC method call to 'ping' service")
	response, err := client.Call("ping", []interface{}{"hello"})
	if err != nil {
		log.Fatalln(err)
	} else {
		log.Println("Ping response:", response)
	}
	for {
		stats := client.Stats()
		i := stats.Reads
		ti := stats.TotalReads
		o := stats.Writes
		to := stats.TotalWrites
		log.Printf("bytes: %d/%d##%d/%d ops: %d/%d##%d/%d err: %d/%d##%d/%d reconnects: %d pings: %d/%d uptime: %v##%v",
			i.Bytes, o.Bytes,
			ti.Bytes, to.Bytes,
			i.Ops, o.Ops,
			ti.Ops, to.Ops,
			i.Errors, o.Errors,
			ti.Errors, to.Errors,
			stats.Reconnects,
			stats.PingsRecv, stats.PingsSent,
			i.Runtime, ti.Runtime)
		log.Println("Collection: builds", len(builds.FindAll()))

		time.Sleep(10 * time.Second)
	}
}
