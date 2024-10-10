package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	sis "github.com/f7ed0/golang_SIS_LWE"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"github.com/vmihailenco/msgpack/v5"
)

type VideoPacketSIS struct {
	MsgPackPacket []byte `msgpack:"packet"`
	A             []byte `msgpack:"a"`
	V             []byte `msgpack:"v"`
}

type VideoPacket struct {
	VideoID      string `msgpack:"video_id"`
	PacketNumber int    `msgpack:"packet_number"`
	TotalPackets int    `msgpack:"total_packets"` // Use 0 if unknown
	Data         []byte `msgpack:"data"`
	A            []byte `msgpack:"a"`
	V            []byte `msgpack:"v"`
}

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
					"format": "avi", "map": "0", "c": "copy",
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
		frameSize := w * h
		buf := make([]byte, frameSize, frameSize)
		sum := 0
		//buf_stream := bufio.NewReaderSize(reader, frameSize*3)
		for {
			/*for buf_stream.Buffered() < frameSize {

				time.Sleep(1 * time.Millisecond)
			}*/
			n, err := io.ReadFull(reader, buf)
			fmt.Println("Read", n, "bytes")
			if n == 0 || err == io.EOF {
				return
			} else if err != nil {
				panic(fmt.Sprintf("read error: %d, %s", n, err))
			}

			sum += 1

			videoPacket := VideoPacket{
				VideoID:      "test",
				PacketNumber: sum,
				TotalPackets: 0,
				Data:         buf,
			}

			msgPackPacket, err := msgpack.Marshal(videoPacket)

			if err != nil {
				panic(fmt.Sprintf("Msgpack error %s", err.Error()))
			}

			matrix_a, matrix_v, err := sis.Default.GenerateCheck(msgPackPacket)

			if err != nil {
				panic(fmt.Sprintf("Sis error %s", err.Error()))
			}

			//sis.Default.Validate(packet, matrix_a, matrix_v)

			matrix_a_bytes := sis.SerializeInts(matrix_a)
			matrix_v_bytes := sis.SerializeInts(matrix_v)

			fmt.Printf("Serialize a %v\n", matrix_a_bytes)
			fmt.Printf("Serialize v %v\n", matrix_v_bytes)

			sis_packet := VideoPacketSIS{
				MsgPackPacket: msgPackPacket,
				A:             matrix_a_bytes,
				V:             matrix_v_bytes,
			}

			sis_packet_bytes, err := msgpack.Marshal(sis_packet)

			client.Publish("go-streaming", 0, false, sis_packet_bytes).Wait()

			time.Sleep(500 * time.Millisecond)

			if err != nil {
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
