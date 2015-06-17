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
	err = client.SubCall("builds", []interface{}{"abc"})
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
		time.Sleep(10 * time.Second)
	}
}
