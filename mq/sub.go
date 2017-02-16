package mq

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Subscriber a rabbitmq worker in work queues model
// If you want only one of your inst receive the msg,
// give a queue name. Or all inst will receive msg
// when you leave Queue empty.
type Subscriber struct {
	Config    Config
	Exchange  string
	Queue     string
	Forwarder DeliveryForwarder
}

// Start a new worker
func (s Subscriber) Start() {
	log.Infof("A new work on %s", s.Queue)
	var conn *amqp.Connection
	defer conn.Close()
	var ch *amqp.Channel
	defer ch.Close()
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
			log.Errorf("Subscriber on %s is terminated,%s", s.Exchange, err)
			return
		}
		err = ch.ExchangeDeclare(
			s.Exchange, // name
			"fanout",   // type
			true,       // durable
			false,      // auto-deleted
			false,      // internal
			false,      // no-wait
			nil,        // arguments
		)
		if err != nil {
			log.Errorf("Subscriber on %s is terminated,%s", s.Exchange, err)
			return
		}
		q, err := ch.QueueDeclare(
			s.Queue, // name
			true,    // durable
			true,    // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)
		if err != nil {
			log.Errorf("Subscriber on %s is terminated,%s", s.Exchange, err)
			return
		}
		err = ch.QueueBind(
			q.Name,     // queue name
			"",         // routing key
			s.Exchange, // exchange
			false,
			nil,
		)
		if err != nil {
			log.Errorf("Subscriber on %s is terminated,%s", s.Exchange, err)
			return
		}
		err = ch.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		if err != nil {
			log.Errorf("Subscriber on %s is terminated,%s", s.Exchange, err)
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
			log.Errorf("Subscriber on %s is terminated,%s", s.Exchange, err)
			return
		}
		// Msgs channel will close when conn is broken, then reconnet.
		for d := range msgs {
			log.Debugf("%s received a msg.", s.Queue)
			go s.Forwarder(d)
		}

	}
}
