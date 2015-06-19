package main

import (
	"log"
	"time"

	"github.com/badslug/ddp"
)

func main() {
	client, err := ddp.NewClient("ws://localhost:3000/websocket", "http://localhost/")
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Close()

	log.Println("Connected", client.Version(), client.Session())
	err = client.Sub("builds", []interface{}{"abc"})
	if err != nil {
		log.Fatalln(err)
	}
	time.Sleep(10 * time.Second)
	response, err := client.Call("ping", []interface{}{"hello"})
	if err != nil {
		log.Fatalln(err)
	} else {
		log.Println("PING", response)
	}
	for {
		stats := client.Stats()
		i := stats.Reads
		ti := stats.TotalReads
		o := stats.Writes
		to := stats.TotalWrites
		log.Printf("b: %d/%d##%d/%d ops: %d/%d##%d/%d err: %d/%d##%d/%d recon: %d pings: %d/%d time: %v##%v",
			i.Bytes, o.Bytes,
			ti.Bytes, to.Bytes,
			i.Ops, o.Ops,
			ti.Ops, to.Ops,
			i.Errors, o.Errors,
			ti.Errors, to.Errors,
			stats.Reconnects,
			stats.PingsRecv, stats.PingsSent,
			i.Runtime, ti.Runtime)
		time.Sleep(1 * time.Minute)
		/*
			log.Println("builds", len(client.Collections["builds"].FindAll()))
			for key, value := range client.Collections["builds"].FindAll() {
				log.Println(key)
				data, _ := json.Marshal(value)
				log.Println(string(data))
			}
		*/
	}
}
