package main

import (
	"fmt"
	"log"
	"os"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Retrieve environment variables
	BROKER_URL := os.Getenv("BROKER_URL")
	BROKER_PORT := os.Getenv("BROKER_PORT")
	BROKER_USER := os.Getenv("BROKER_USERNAME")
	BROKER_PASSWORD := os.Getenv("BROKER_PASSWORD")
	BROKER_STREAMING_CLIENT_ID := os.Getenv("BROKER_STREAMING_CLIENT_ID")

	// Set up MQTT client options
	var clientOptions = mqtt.NewClientOptions()
	clientOptions.AddBroker(fmt.Sprintf("tcp://%s:%s", BROKER_URL, BROKER_PORT))
	clientOptions.SetClientID(BROKER_STREAMING_CLIENT_ID)
	clientOptions.SetUsername(BROKER_USER)
	clientOptions.SetPassword(BROKER_PASSWORD)

	client := mqtt.NewClient(clientOptions)
	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", token.Error())
	}
	log.Println("Connected to MQTT broker")

	RunExampleStream("videos/test.mp4", client)

	client.Disconnect(250)
	log.Println("Disconnected from MQTT broker")
}
