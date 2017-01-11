package mq

import (
	"time"

	log "github.com/Sirupsen/logrus"
)

// RPC a rabbitmq processor in rpc model
type RPC struct {
	Config    Config
	Queue     string
	Processor DeliveryProcessor
}

// Start a new rpc
func (r RPC) Start() {
	log.Infof("A new rpc on %s", r.Queue)
	var conn *amqp.Connection
	defer conn.Close()
	var ch *amqp.Channel
	defer ch.Close()
	var err error
	for {
		conn, err = amqp.Dial(r.Config.URL())
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
		q, err := ch.QueueDeclare(
			r.Queue, // name
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
		err = ch.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		if err != nil {
			log.Error(err, "Retry in 2 seconds")
			time.Sleep(time.Second * 2)
			continue
		}
		msgs, err := ch.Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		if err != nil {
			log.Error(err, "Retry in 2 seconds")
			time.Sleep(time.Second * 2)
			continue
		}
		for d := range msgs {
			log.Debugf("%s received a msg:%+v", r.Queue, d)
			pub := r.Processor(d)
			err := ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				pub,       // Publishing
			)
			if err != nil {
				log.Error(err, "send rpc reply fail")
			}
		}

	}
}
