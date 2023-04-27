package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

type DeviceData struct {
	ID        int    `json:"id"`
	Data      string `json:"data"`
	Timestamp int64  `json:"timestamp"`
	UserID    int    `json:"user_id"`
	DeviceID  string `json:"device_id"`
}

func main() {

	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("failed to load environment variables: %v", err)
	}

	conn, err := amqp.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}
	defer ch.Close()

	exchangeName := "data"
	exchangeType := "fanout"

	err = ch.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare exchange: %v", err)
	}

	queueName := "data_queue"

	_, err = ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare queue: %v", err)
	}

	err = ch.QueueBind(
		queueName,
		"",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to bind queue to exchange: %v", err)
	}

	port := os.Getenv("TCP_SERVER_PORT")

	l, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	log.Printf("listening on port %s...", port)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("failed to accept connection: %v", err)
			continue
		}

		go handleConnection(conn, ch, exchangeName)
	}
}

func handleConnection(conn net.Conn, ch *amqp.Channel, exchangeName string) {
	defer conn.Close()

	log.Printf("accepted connection from %s", conn.RemoteAddr())

	dec := json.NewDecoder(conn)

	for {
		var data DeviceData
		err := dec.Decode(&data)
		if err != nil {
			log.Printf("failed to decode data: %v", err)
			break
		}

		log.Printf("received data from device %d: %s", data.ID, data.Data)

		err = ch.Publish(
			exchangeName,
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(data.Data),
			},
		)
		if err != nil {
			log.Printf("failed to publish message to exchange: %v", err)
		}
	}
}
