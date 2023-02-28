package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	ec "github.com/reb-felipe/eventcounter/pkg"
	"github.com/streadway/amqp"
)

const (
	idleTime = 5 * time.Second
)

type AtomicMap map[ec.EventType]map[string]int

// Checks if a counter exists for that event exists if not, creates one
func (am AtomicMap) Create(et ec.EventType, usrID string) {
	atomicRW.Lock()
	defer atomicRW.Unlock()
	userEventCounter[et] = make(map[string]int)
}

// Checks if a counter exists for that event exists if not, creates one
func (am AtomicMap) Exists(et ec.EventType, usrID string) bool {
	atomicRW.RLock()
	defer atomicRW.RUnlock()
	if _, ok := userEventCounter[et]; !ok {
		return false
	}
	return true
}

// Increments the user's event counter
func (am AtomicMap) Increment(et ec.EventType, usrID string) {
	atomicRW.Lock()
	defer atomicRW.Unlock()
	userEventCounter[et][usrID]++
}

var userEventCounter AtomicMap
var processedMessages map[string]bool

// Creating RW mutex for concurrency in maps
var atomicRW sync.RWMutex
var processedMessagesRW sync.RWMutex

// Define the worker function
func worker(ctx context.Context, ch chan ec.Message, eventType ec.EventType, wg *sync.WaitGroup) {
	defer wg.Done()

	// Start processing messages
	for {
		select {
		// Read messages from the channel
		case msg := <-ch:
			// Checks if the message was already processed
			if !isProcessed(msg.UID) {

				// Checks and creates the counter
				if !userEventCounter.Exists(eventType, msg.UserID) {
					userEventCounter.Create(eventType, msg.UserID)
				}

				// Increment the user event counter
				userEventCounter.Increment(eventType, msg.UserID)

				// Sets the message as processed
				setProcessed(msg.UID)

				// Print the user event counter
				log.Printf("%s %s %s: %d", eventType, msg.UserID, msg.UID, userEventCounter[eventType][msg.UserID])
			}

		// Check if the worker should stop
		case <-ctx.Done():
			log.Printf("Finished processing %s messages", eventType)
			return
		}
	}

}

// Returns if a message has already been processed
func isProcessed(uid string) bool {
	processedMessagesRW.RLock()
	defer processedMessagesRW.RUnlock()
	_, ok := processedMessages[uid]
	return ok
}

// Sets a message as processed
func setProcessed(uid string) {
	processedMessagesRW.Lock()
	defer processedMessagesRW.Unlock()
	processedMessages[uid] = true
}

// Writes the userEventCounter map into separate json files
func writeToFile() {
	// Looping through the map userEventCounter and writing it into file
	for eventType, v := range userEventCounter {
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

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Open a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Initialize the user event counter
	userEventCounter = make(map[ec.EventType]map[string]int)

	// Initialize the processed messages map
	processedMessages = make(map[string]bool)

	// Create channels for each event type
	createdCh := make(chan ec.Message)
	updatedCh := make(chan ec.Message)
	deletedCh := make(chan ec.Message)

	// Creating WaitGroup to sync the end of all workers
	var wg sync.WaitGroup
	wg.Add(3)

	// Creating a ctx to alert workers after time have expired
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	// Start a worker for each event type
	go worker(ctx, createdCh, ec.EventCreated, &wg)
	go worker(ctx, updatedCh, ec.EventUpdated, &wg)
	go worker(ctx, deletedCh, ec.EventDeleted, &wg)

	// Start consuming messages
	msgs, err := ch.Consume(
		"eventcountertest", // queue
		"",                 // consumer
		false,              // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	// Start 5 seconds timer to identify idle
	idleTimer := time.NewTimer(idleTime)

loop:
	for {
		select {
		case message := <-msgs:
			parts := strings.Split(message.RoutingKey, ".")
			if len(parts) != 3 {
				log.Printf("Invalid routing key: %s", message.RoutingKey)
			}

			userID := parts[0]
			eventType := ec.EventType(parts[2])

			// Unmarshal the message body to a Message struct
			msg := ec.Message{
				UID:       string(message.Body),
				EventType: ec.EventType(eventType),
				UserID:    userID,
			}

			// Send the message to the channel for its event type
			switch eventType {
			case ec.EventCreated:
				createdCh <- msg
			case ec.EventUpdated:
				updatedCh <- msg
			case ec.EventDeleted:
				deletedCh <- msg
			default:
				log.Printf("Unknown event type: %s", eventType)
			}

			// Reset the idle timer
			idleTimer.Reset(idleTime)

		case <-idleTimer.C:
			log.Printf("Idle time exceeded")
			cancelCtx()
			break loop
		}
	}
	// Wait for all workers to finish their current task
	wg.Wait()

	// Writing files into json
	writeToFile()
}
