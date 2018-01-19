package mq

import (
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

// RPCClient a rabbitmq worker in work queues model
type RPCClient struct {
	Config Config
	Queue  string
	conn   *amqp.Connection
	ready  bool
	sync.RWMutex
}

func (c *RPCClient) isReady() bool {
	c.RLock()
	defer c.RUnlock()
	return c.ready
}

func (c *RPCClient) getConn() *amqp.Connection {
	c.RLock()
	defer c.RUnlock()
	return c.conn
}

func (c *RPCClient) connect() {
	c.Lock()
	c.ready = false
	c.Unlock()
	for {
		conn, err := amqp.Dial(c.Config.URL())
		if err != nil {
			log.Error(err, "Retry in 2 seconds")
			time.Sleep(time.Second * 2)
			continue
		}
		c.Lock()
		if c.conn != nil {
			c.conn.Close()
		}
		c.conn = conn
		c.ready = true
		c.Unlock()
		return
	}
}

// Start init client connection
func (c *RPCClient) Start() {
	c.connect()
	log.Infof("A new rpc client connect to %s", c.Queue)
}

// Send Send Msg and waiting reply
func (c *RPCClient) Send(msg []byte) (reply []byte, err error) {
	if !c.isReady() {
		return []byte{}, fmt.Errorf("queue %s is not ready for use", c.Queue)
	}
	corr ,_:= uuid.NewV1()
	corrID:=corr.String()
	// receive channel
	conn := c.getConn()
	ch, err := conn.Channel()
	defer ch.Close()
	if err != nil {
		go c.connect()
		return []byte{}, fmt.Errorf("make receive channel failed: %s", err)
	}
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		go c.connect()
		return []byte{}, fmt.Errorf("require a tmp reply queue failed: %s", err)
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
		// consume 不会因为没有连接而失败，所以不尝试重连
		return []byte{}, fmt.Errorf("require a tmp reply queue failed: %s", err)
	}
	err = ch.Publish(
		"",      // exchange
		c.Queue, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			Timestamp:     time.Now(),
			ContentType:   "text/plain",
			Body:          msg,
			CorrelationId: corrID,
			ReplyTo:       q.Name,
		})
	if err != nil {
		go c.connect()
		return []byte{}, fmt.Errorf("send msg failed: %s", err)
	}
	for {
		select {
		case d := <-msgs:
			if corrID != d.CorrelationId {
				log.Errorf("msg correlationID : %s  invalide correlationID %s", corrID, d.CorrelationId)
				continue
			}
			return d.Body, nil
		case <-time.After(time.Second * 3):
			return []byte{}, fmt.Errorf("rpc reply timeout")
		}
	}

}
