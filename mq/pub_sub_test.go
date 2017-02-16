package mq

import (
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
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
			Host:     "wrong_host",
			Port:     "5672",
		},
		Exchange: "veinlib_test_exchange",
	}
	go pub.Start(sender)
	time.Sleep(time.Second * 3)
	ok := pub.IsReady()
	if ok {
		t.Errorf("connect a wrong host")
	}
	pub.Lock()
	pub.Config.Host = host
	pub.Unlock()
	time.Sleep(time.Second * 3)
	ok = pub.IsReady()
	if !ok {
		t.Errorf("someting wrong,pub not start.")
	}
	var sub = Subscriber{
		Config: Config{
			Username: "guest",
			Password: "guest",
			Host:     host,
			Port:     "5672",
		},
		Queue:     "veinlib_test_queue",
		Exchange:  "veinlib_test_exchange",
		Forwarder: fwd,
	}
	go sub.Start()
	// waiting for sub start
	time.Sleep(time.Millisecond * 100)
	sender <- []byte("Hello World!")
	log.Debug("Sent")
	rcv := <-pipe
	log.Debug("Received")
	if string(rcv) != "Hello World!" {
		t.Errorf("delivery fail, src:%s ,dst:%s", "Hello World!", rcv)
	}
}

func fwd(d amqp.Delivery) {
	pipe <- d.Body
}
