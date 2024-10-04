package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
	ffmpeg "github.com/u2takey/ffmpeg-go"
)

func loadClient() mqtt.Client {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	BROKER_URL := os.Getenv("BROKER_URL")
	BROKER_PORT := os.Getenv("BROKER_PORT")
	BROKER_USER := os.Getenv("BROKER_USER")
	BROKER_PASSWORD := os.Getenv("BROKER_PASSWORD")
	//BROKER_STREAMING_CLIENT_ID := os.Getenv("BROKER_STREAMING_CLIENT_ID")

	var clientOptions = mqtt.NewClientOptions()
	clientOptions.AddBroker(fmt.Sprintf("%s:%s", BROKER_URL, BROKER_PORT))
	clientOptions.SetClientID("reader-id")
	clientOptions.SetUsername(BROKER_USER)
	clientOptions.SetPassword(BROKER_PASSWORD)
	var client = mqtt.NewClient(clientOptions)

	token := client.Connect()
	token.Wait()

	return client
}

func TestReadFromMQtt(t *testing.T) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	BROKER_STREAMING_TOPIC := os.Getenv("BROKER_STREAMING_TOPIC")

	fmt.Printf("Reading from topic %s\n", BROKER_STREAMING_TOPIC)
	client := loadClient()

	pr, pw := io.Pipe()
	count := 0
	token := client.Subscribe(BROKER_STREAMING_TOPIC, 0, func(client mqtt.Client, msg mqtt.Message) {
		_, err := pw.Write(msg.Payload())

		count++
		fmt.Printf("Received %d messages\n", count)
		if err != nil {
			log.Fatal(err)
		}

		if (string(msg.Payload())) == "EOSTREAMING" {
			pw.Close()
		}
	})
	token.Wait()

	done := make(chan error)
	go func() {
		err := ffmpeg.Input("pipe:",
			ffmpeg.KwArgs{"format": "rawvideo",
				"pix_fmt": "rgb24", "s": fmt.Sprintf("%dx%d", 1920, 1080),
			}).
			Output("videos/out.mp4", ffmpeg.KwArgs{"pix_fmt": "yuv420p"}).
			OverWriteOutput().
			WithInput(pr).
			Run()
		log.Println("ffmpeg process2 done")
		done <- err
		close(done)
	}()

	err = <-done
	if err != nil {
		panic(err)
	}
}
