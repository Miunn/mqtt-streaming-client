package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	sis "github.com/f7ed0/golang_SIS_LWE"
	"github.com/google/uuid"
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
}

func getVideoSize(fileName string) (int, int) {
	log.Println("Getting video size for", fileName)
	data, err := ffmpeg.Probe(fileName)
	if err != nil {
		panic(err)
	}
	log.Println("Got video info:", data)
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
	log.Println("Starting FFmpeg process with fragmented MP4 format")
	done := make(chan error)
	go func() {
		err := ffmpeg.Input(infileName).
			Output("pipe:",
				ffmpeg.KwArgs{
					"format":   "mp4",
					"movflags": "frag_keyframe+empty_moov+default_base_moof",
					"map":      "0",
					"c":        "copy",
				}).
			WithOutput(writer).
			Run()
		log.Println("FFmpeg process done")
		_ = writer.Close()
		done <- err
		close(done)
	}()
	return done
}

func process(reader io.ReadCloser, client mqtt.Client, w, h int) {
	go func() {
		frameSize := w * h * 3
		buf := make([]byte, frameSize)
		videoID := uuid.New().String()
		packetNumber := 0
		totalFrames := 0

		for {
			n, err := io.ReadFull(reader, buf)
			if n > 0 {
				packetNumber++
				totalFrames++
				log.Printf("Frame %d: Read %d bytes\n", packetNumber, n)

				videoPacket := VideoPacket{
					VideoID:      videoID,
					PacketNumber: packetNumber,
					TotalPackets: 0,
					Data:         append([]byte{}, buf...),
				}

				videoPacketSIS, err := msgpack.Marshal(encodeSISPacket(videoPacket))
				if err != nil {
					log.Fatalf("Msgpack error: %s", err.Error())
				}

				client.Publish(fmt.Sprintf("video/stream/%s", videoID), 0, false, videoPacketSIS).Wait()

				log.Printf("Total Frames Sent: %d\n", totalFrames)
			}

			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					log.Printf("Finished processing video. Total frames: %d\n", totalFrames)
					return
				}
				log.Fatalf("Read error: %s", err)
			}
		}
	}()
}

func encodeSISPacket(packet VideoPacket) VideoPacketSIS {
	packetSIS := VideoPacketSIS{
		MsgPackPacket: []byte{},
		A:             []byte{},
		V:             []byte{},
	}

	msgPackPacket, err := msgpack.Marshal(packet)
	if err != nil {
		panic(fmt.Sprintf("Msgpack error: %s", err.Error()))
	}

	matrix_a, matrix_v, err := sis.Default.GenerateCheck(msgPackPacket)
	if err != nil {
		panic(fmt.Sprintf("SIS error: %s", err.Error()))
	}

	matrix_a_bytes := sis.SerializeInts(matrix_a)
	matrix_v_bytes := sis.SerializeInts(matrix_v)

	packetSIS.MsgPackPacket = msgPackPacket
	packetSIS.A = matrix_a_bytes
	packetSIS.V = matrix_v_bytes
	return packetSIS
}

func RunExampleStream(inFile string, client mqtt.Client) {
	w, h := getVideoSize(inFile)
	log.Printf("Video Width: %d, Height: %d\n", w, h)

	pr1, pw1 := io.Pipe()

	done1 := startFFmpegProcess1(inFile, pw1)
	process(pr1, client, w, h)

	err := <-done1
	if err != nil {
		panic(err)
	}
	log.Println("FFmpeg process completed")

	client.Publish("go-streaming", 0, false, []byte("EOSTREAMING")).Wait()
	log.Println("Sent EOS")
}
