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

	BROKER_URL := os.Getenv("BROKER_URL")
	BROKER_PORT := os.Getenv("BROKER_PORT")
	BROKER_USER := os.Getenv("BROKER_USER")
	BROKER_PASSWORD := os.Getenv("BROKER_PASSWORD")
	BROKER_STREAMING_CLIENT_ID := os.Getenv("BROKER_STREAMING_CLIENT_ID")

	var clientOptions = mqtt.NewClientOptions()
	clientOptions.AddBroker(fmt.Sprintf("%s:%s", BROKER_URL, BROKER_PORT))
	clientOptions.SetClientID(BROKER_STREAMING_CLIENT_ID)
	clientOptions.SetUsername(BROKER_USER)
	clientOptions.SetPassword(BROKER_PASSWORD)
	var client = mqtt.NewClient(clientOptions)

	token := client.Connect()
	token.Wait()
	fmt.Println("Connect: ", token.Error())

	RunExampleStream("videos/test.mp4", client)
}
