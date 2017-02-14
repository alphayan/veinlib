package mq

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Worker a rabbitmq worker in work queues model
type Worker struct {
	Config    Config
	Queue     string
	Forwarder DeliveryForwarder
}

// Start a new worker
func (w Worker) Start() {
	log.Infof("A new work on %s", w.Queue)
	var conn *amqp.Connection
	defer conn.Close()
	var ch *amqp.Channel
	defer ch.Close()
	var err error
	for {
		conn, err = amqp.Dial(w.Config.URL())
		if err != nil {
			log.Error(err, "Retry in 2 seconds")
			time.Sleep(time.Second * 2)
			continue
		}
		log.Info("RabbitMQ connect successful.")
		// receive channel
		ch, err = conn.Channel()
		if err != nil {
			log.Errorf("Worker on %s is terminated,%s", w.Queue, err)
			return
		}
		q, err := ch.QueueDeclare(
			w.Queue, // name
			true,    // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)
		if err != nil {
			log.Errorf("Worker on %s is terminated,%s", w.Queue, err)
			return
		}
		err = ch.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		if err != nil {
			log.Errorf("Worker on %s is terminated,%s", w.Queue, err)
			return
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
			log.Errorf("Worker on %s is terminated,%s", w.Queue, err)
			return
		}
		for d := range msgs {
			//log.Debugf("%s received a msg.", w.Queue)
			go w.Forwarder(d)
		}

	}
}
