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
	client.Write([]byte("{\"msg\":\"sub\",\"id\":\"100\",\"name\":\"builds\",\"params\":[\"abc\"]}"))
	time.Sleep(10 * time.Second)
	client.Write([]byte("{\"msg\":\"method\",\"method\":\"ping\",\"params\":[\"hello\"],\"id\":\"200\"}"))
	for {
		time.Sleep(10 * time.Second)
	}
}
