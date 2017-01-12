package mq

import (
	"testing"

	"github.com/streadway/amqp"
)

var pipe chan []byte

func TestPubSub(t *testing.T) {
	// start pub and sub
	var sender chan []byte
	sender = make(chan []byte)
	pipe = make(chan []byte)
	var pub = Publisher{
		Config: Config{
			Username: "guest",
			Password: "guest",
			Host:     "localhost",
			Port:     "5672",
		},
		Exchange: "veinlib_test_exchange",
	}
	go pub.Start(sender)
	var sub = Subscriber{
		Config: Config{
			Username: "guest",
			Password: "guest",
			Host:     "localhost",
			Port:     "5672",
		},
		Queue:     "veinlib_test_queue",
		Exchange:  "veinlib_test_exchange",
		Forwarder: fwd,
	}
	go sub.Start()
	sender <- []byte("Hello World!")
	rcv := <-pipe
	if string(rcv) != "Hello World!" {
		t.Errorf("delivery fail, src:%s ,dst:%s", "Hello World!", rcv)
	}
}

func fwd(d amqp.Delivery) {
	pipe <- d.Body
}
