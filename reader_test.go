package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	sis "github.com/f7ed0/golang_SIS_LWE"
	"github.com/joho/godotenv"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"github.com/vmihailenco/msgpack/v5"
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
		var packetSIS VideoPacketSIS

		fmt.Printf("Received packet\n")
		err := msgpack.Unmarshal(msg.Payload(), &packetSIS)
		fmt.Printf("Received packet %v\n", packetSIS.V)

		a, err := sis.DeserializeInts(packetSIS.A, sis.Default.M*sis.Default.N)
		fmt.Printf("Deserialized a %v\n", a)

		if err != nil {
			fmt.Println("Error deserializing a", err.Error())
			pw.Close()
			return
		}

		v, err := sis.DeserializeInts(packetSIS.V, sis.Default.N*1)
		fmt.Printf("Deserialized v\n")

		if err != nil {
			fmt.Println("Error deserializing v", err.Error())
			pw.Close()
			return
		}

		ok, err := sis.Default.Validate(packetSIS.MsgPackPacket, a, v)
		fmt.Printf("Validate\n")

		if err != nil {
			panic(fmt.Sprintf("Validation error %s", err.Error()))
		}

		if !ok {
			panic("Validation failed")
		}

		var packet VideoPacket

		err = msgpack.Unmarshal(packetSIS.MsgPackPacket, &packet)
		fmt.Printf("Unmarshalled\n")

		_, err = pw.Write(packet.Data)
		fmt.Printf("Wrote\n")

		count++
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
			ffmpeg.KwArgs{
				"f": "avi",
			}).
			Output("videos/out.avi").
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
