package main

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConsumerSVC(t *testing.T) {
	c := NewConsumerSVC()
	assert.NotNil(t, c)
	assert.NotNil(t, c.ProcessedMessages)
	assert.NotNil(t, c.UserEventCounter)
	assert.Empty(t, c.ProcessedMessages)
	assert.Empty(t, c.UserEventCounter)
}

func TestIncrement(t *testing.T) {
	c := NewConsumerSVC()
	c.Increment("created", "user1")
	assert.Equal(t, 1, c.UserEventCounter["created"]["user1"])

	c.Increment("created", "user1")
	assert.Equal(t, 2, c.UserEventCounter["created"]["user1"])

	c.Increment("updated", "user1")
	assert.Equal(t, 1, c.UserEventCounter["updated"]["user1"])
}

func TestIsProcessedAndSetProcessed(t *testing.T) {
	c := NewConsumerSVC()
	// uid is not processed initially
	assert.False(t, c.IsProcessed("uid1"))

	// Set uid1 as processed
	c.SetProcessed("uid1")
	assert.True(t, c.IsProcessed("uid1"))

	// Set uid2 as processed
	c.SetProcessed("uid2")
	assert.True(t, c.IsProcessed("uid2"))

	// uid1 and uid2 are now processed
	assert.True(t, c.IsProcessed("uid1"))
	assert.True(t, c.IsProcessed("uid2"))
}

func TestCreated(t *testing.T) {
	c := NewConsumerSVC()
	ctx := context.WithValue(context.Background(), "usrid", "user1")
	// uid1 is not processed initially
	assert.False(t, c.IsProcessed("uid1"))

	// call Created function
	err := c.Created(ctx, "uid1")
	assert.NoError(t, err)

	// uid1 is now processed
	assert.True(t, c.IsProcessed("uid1"))

	// the user event counter is incremented for user1 and "created" event
	assert.Equal(t, 1, c.UserEventCounter["created"]["user1"])
}

func TestUpdated(t *testing.T) {
	c := NewConsumerSVC()
	ctx := context.WithValue(context.Background(), "usrid", "user1")
	// uid1 is not processed initially
	assert.False(t, c.IsProcessed("uid1"))

	// call Created function
	err := c.Updated(ctx, "uid1")
	assert.NoError(t, err)

	// uid1 is now processed
	assert.True(t, c.IsProcessed("uid1"))

	// the user event counter is incremented for user1 and "created" event
	assert.Equal(t, 1, c.UserEventCounter["updated"]["user1"])
}

func TestDeleted(t *testing.T) {
	c := NewConsumerSVC()
	ctx := context.WithValue(context.Background(), "usrid", "user1")
	// uid1 is not processed initially
	assert.False(t, c.IsProcessed("uid1"))

	// call Created function
	err := c.Deleted(ctx, "uid1")
	assert.NoError(t, err)

	// uid1 is now processed
	assert.True(t, c.IsProcessed("uid1"))

	// the user event counter is incremented for user1 and "created" event
	assert.Equal(t, 1, c.UserEventCounter["deleted"]["user1"])
}

func TestWriteToFile(t *testing.T) {
	c := NewConsumerSVC()
	c.UserEventCounter = map[string]map[string]int{
		"created": {"user1": 1, "user2": 2},
	}
	c.writeToFile()

	// Verify that the file was created and contains the expected data
	file, err := os.Open("created.json")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	var data map[string]int
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		t.Fatalf("Failed to read from file: %v", err)
	}

	if !reflect.DeepEqual(data, map[string]int{"user1": 1, "user2": 2}) {
		t.Errorf("Unexpected file contents: %v", data)
	}
}
