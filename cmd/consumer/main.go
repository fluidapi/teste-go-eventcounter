package main

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	ec "github.com/reb-felipe/eventcounter/pkg"
	"github.com/streadway/amqp"
)

const (
	idleTime = 5 * time.Second
)

// Define the worker function
func worker(ctx context.Context, ch chan ec.Message, eventType ec.EventType, wg *sync.WaitGroup, consumer ec.Consumer) {
	defer wg.Done()

	// Start processing messages
	for {
		select {
		// Read messages from the channel
		case msg := <-ch:
			consumerContext := context.WithValue(context.TODO(), "usrid", msg.UserID)
			switch eventType {
			case ec.EventCreated:
				consumer.Created(consumerContext, msg.UID)
			case ec.EventUpdated:
				consumer.Updated(consumerContext, msg.UID)
			case ec.EventDeleted:
				consumer.Deleted(consumerContext, msg.UID)
			}

		// Check if the worker should stop
		case <-ctx.Done():
			log.Printf("Finished processing %s messages", eventType)
			return
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

	// Get an instant of a ConsumerSVC that implements Consumer Interface
	c := NewConsumerSVC()

	// Instantiate ConsumerWrapper to use it's methods
	consumer := &ec.ConsumerWrapper{Consumer: c}

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
	go worker(ctx, createdCh, ec.EventCreated, &wg, consumer)
	go worker(ctx, updatedCh, ec.EventUpdated, &wg, consumer)
	go worker(ctx, deletedCh, ec.EventDeleted, &wg, consumer)

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
	c.writeToFile()
}
