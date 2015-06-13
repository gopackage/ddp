package main

import (
	"log"

	"github.com/badslug/ddp"
)

func main() {
	client, err := ddp.NewClient("ws://localhost:3000/websocket", "http://localhost/")
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Close()

	log.Println("Connected", client.Version(), client.Session())
}
