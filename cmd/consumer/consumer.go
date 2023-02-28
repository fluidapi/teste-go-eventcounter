package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)

type ConsumerSVC struct {
	ProcessedMessages map[string]bool
	UserEventCounter  map[string]map[string]int
	MuEC              sync.Mutex
	MuPM              sync.Mutex
}

func NewConsumerSVC() *ConsumerSVC {
	return &ConsumerSVC{
		ProcessedMessages: make(map[string]bool),
		UserEventCounter:  make(map[string]map[string]int),
	}
}

func (c *ConsumerSVC) Increment(event string, usrid string) {
	c.MuEC.Lock()
	defer c.MuEC.Unlock()
	// Checks if the counter already exists for the user
	if _, ok := c.UserEventCounter[event]; !ok {
		c.UserEventCounter[event] = make(map[string]int)
	}
	c.UserEventCounter[event][usrid]++
}

func (c *ConsumerSVC) Created(ctx context.Context, uid string) error {
	usrid := ctx.Value("usrid").(string)
	if !c.IsProcessed(uid) {

		c.Increment("created", usrid)

		// Sets the message as processed
		c.SetProcessed(uid)

		// Print the user event counter
		log.Printf("%s %s %s: %d", "created", usrid, uid, c.UserEventCounter["created"][usrid])
	}
	return nil
}

func (c *ConsumerSVC) Updated(ctx context.Context, uid string) error {
	usrid := ctx.Value("usrid").(string)
	if !c.IsProcessed(uid) {

		c.Increment("updated", usrid)

		// Sets the message as processed
		c.SetProcessed(uid)

		// Print the user event counter
		log.Printf("%s %s %s: %d", "updated", usrid, uid, c.UserEventCounter["updated"][usrid])
	}
	return nil
}

func (c *ConsumerSVC) Deleted(ctx context.Context, uid string) error {
	usrid := ctx.Value("usrid").(string)
	if !c.IsProcessed(uid) {

		c.Increment("deleted", usrid)

		// Sets the message as processed
		c.SetProcessed(uid)

		// Print the user event counter
		log.Printf("%s %s %s: %d", "deleted", usrid, uid, c.UserEventCounter["deleted"][usrid])
	}
	return nil
}

// Writes the UserEventCounter map into separate json files
func (c *ConsumerSVC) writeToFile() {
	// Looping through the map UserEventCounter and writing it into file
	for eventType, v := range c.UserEventCounter {
		// Opens the file with the eventType's name
		filename := fmt.Sprintf("%s.json", string(eventType))
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("Failed to create file: %v", err)
		}
		defer file.Close()

		// Writes each user into the file
		encoder := json.NewEncoder(file)
		if err := encoder.Encode(v); err != nil {
			log.Fatalf("Failed to write to file: %v", err)
		}
	}
}

// Returns if a message has already been processed
func (c *ConsumerSVC) IsProcessed(uid string) bool {
	c.MuPM.Lock()
	defer c.MuPM.Unlock()
	_, ok := c.ProcessedMessages[uid]
	return ok
}

// Sets a message as processed
func (c *ConsumerSVC) SetProcessed(uid string) {
	c.MuPM.Lock()
	defer c.MuPM.Unlock()
	c.ProcessedMessages[uid] = true
}
