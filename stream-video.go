package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	ffmpeg "github.com/u2takey/ffmpeg-go"
)

func getVideoSize(fileName string) (int, int) {
	log.Println("Getting video size for", fileName)
	data, err := ffmpeg.Probe(fileName)
	if err != nil {
		panic(err)
	}
	log.Println("got video info", data)
	type VideoInfo struct {
		Streams []struct {
			CodecType string `json:"codec_type"`
			Width     int
			Height    int
		} `json:"streams"`
	}
	vInfo := &VideoInfo{}
	err = json.Unmarshal([]byte(data), vInfo)
	if err != nil {
		panic(err)
	}
	for _, s := range vInfo.Streams {
		if s.CodecType == "video" {
			return s.Width, s.Height
		}
	}
	return 0, 0
}

func startFFmpegProcess1(infileName string, writer io.WriteCloser) <-chan error {
	log.Println("Starting ffmpeg process1")
	done := make(chan error)
	go func() {
		err := ffmpeg.Input(infileName).
			Output("pipe:",
				ffmpeg.KwArgs{
					"format": "rawvideo", "pix_fmt": "rgb24",
				}).
			WithOutput(writer).
			Run()
		log.Println("ffmpeg process1 done")
		_ = writer.Close()
		done <- err
		close(done)
	}()
	return done
}

func process(reader io.ReadCloser, client mqtt.Client, w, h int) {
	go func() {
		frameSize := w * h * 3
		buf := make([]byte, frameSize, frameSize)
		sum := 0
		for {
			n, err := io.ReadFull(reader, buf)
			if n == 0 || err == io.EOF {
				return
			} else if n != frameSize || err != nil {
				panic(fmt.Sprintf("read error: %d, %s", n, err))
			}

			sum += 1

			sent := client.Publish("go-streaming", 0, false, buf).Wait()
			fmt.Printf("Sent %d frames for a total of 16264 (%d messages)\n", 30*sum, sum)
			if !sent {
				panic(fmt.Sprintf("failed to send frame"))
			}
			// Sleep for 1 second
			time.Sleep(10 * time.Millisecond)

			if n != frameSize || err != nil {
				panic(fmt.Sprintf("write error: %d, %s", n, err))
			}
		}
	}()
	return
}

func RunExampleStream(inFile string, client mqtt.Client) {
	w, h := getVideoSize(inFile)
	log.Println(w, h)

	pr1, pw1 := io.Pipe()

	done1 := startFFmpegProcess1(inFile, pw1)
	process(pr1, client, w, h)

	err := <-done1
	if err != nil {
		panic(err)
	}
	log.Println("Done")

	client.Publish("go-streaming", 0, false, "EOSTREAMING").Wait()
	log.Println("Sent EOS")
}
