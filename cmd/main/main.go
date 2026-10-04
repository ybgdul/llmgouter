package main

import (
	"log"
	"llmgouter/server"
)
 
func main() { 
	log.Println("Starting the application")
	if err := server.Run(); err != nil { 
		log.Fatalf("Could not start the server: %v", err)
	}
}