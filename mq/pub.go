package mq

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Publisher a rabbitmq worker in work queues model
type Publisher struct {
	Config   Config
	Exchange string
}

// Start a new worker
func (p Publisher) Start(payloadchan chan []byte) {
	log.Infof("A new publisher on %s", p.Exchange)
	ch := p.getChannel()
	defer ch.Close()
	defer log.Errorf("publisher of %s terminate!", p.Exchange)
	var err error
	for payload := range payloadchan {
		for {
			err = ch.Publish(
				p.Exchange, // exchange
				"",         // routing key
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					Timestamp:   time.Now(),
					ContentType: "text/plain",
					Body:        payload,
				})
			if err != nil {
				log.Error(err, "connection refused,reconnecting...")
				ch = p.getChannel()
				continue
			}
			log.Debugf("send a payload to %s", p.Exchange)
			break
		}
	}
}

func (p Publisher) getChannel() *amqp.Channel {
	var conn *amqp.Connection
	var ch *amqp.Channel
	var err error
	for {
		log.Debug("mq url: ", p.Config.URL())
		conn, err = amqp.Dial(p.Config.URL())
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
		err = ch.ExchangeDeclare(
			p.Exchange, // name
			"fanout",   // type
			true,       // durable
			false,      // auto-deleted
			false,      // internal
			false,      // no-wait
			nil,        // arguments
		)
		if err != nil {
			log.Error(err, "Retry in 2 seconds")
			time.Sleep(time.Second * 2)
			continue
		}
		return ch
	}
}
