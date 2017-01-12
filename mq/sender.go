package mq

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Sender a rabbitmq worker in work queues model
type Sender struct {
	Config Config
	Queue  string
}

// Start a new worker
func (s Sender) Start(payloadchan chan []byte) {
	log.Infof("A new sender on %s", s.Queue)
	ch := s.getChannel()
	defer ch.Close()
	var err error
	for payload := range payloadchan {
		for {
			err = ch.Publish(
				"",      // exchange
				s.Queue, // routing key
				false,   // mandatory
				false,   // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        payload,
				})
			if err != nil {
				log.Error(err, "connection refused,reconnecting...")
				ch = s.getChannel()
				continue
			}
			break
		}
	}
}

func (s Sender) getChannel() *amqp.Channel {
	var conn *amqp.Connection
	var ch *amqp.Channel
	var err error
	for {
		conn, err = amqp.Dial(s.Config.URL())
		if err != nil {
			log.Error(err, "Retry in 2 seconds")
			time.Sleep(time.Second * 2)
			continue
		}
		log.Info("RabbitMQ connect successful.")
		// receive channel
		ch, err = conn.Channel()
		if err != nil {
			log.Error(err, "Retry in 2 seconds")
			time.Sleep(time.Second * 2)
			continue
		}
		_, err = ch.QueueDeclare(
			s.Queue, // name
			true,    // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)
		if err != nil {
			log.Error(err, "Retry in 2 seconds")
			time.Sleep(time.Second * 2)
			continue
		}
		return ch
	}
}
