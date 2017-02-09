package mq

import (
	"errors"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

// RPCClient a rabbitmq worker in work queues model
type RPCClient struct {
	Config Config
	sync.RWMutex
	PayloadChan chan []byte
	ReplyChan   chan []byte
	Queue       string
}

// Start a new worker
func (rpcClient *RPCClient) Start() {
	rpcClient.PayloadChan = make(chan []byte)
	rpcClient.ReplyChan = make(chan []byte)
	log.Infof("A new rpc on %s", rpcClient.Queue)
	ch, q, msgs := rpcClient.getChannel()
	defer ch.Close()
	var err error
	for payload := range rpcClient.PayloadChan {
		for {
			corrID := uuid.NewV1().String()
			err = ch.Publish(
				"",              // exchange
				rpcClient.Queue, // routing key
				false,           // mandatory
				false,           // immediate
				amqp.Publishing{
					Timestamp:     time.Now(),
					ContentType:   "text/plain",
					Body:          payload,
					CorrelationId: corrID,
					ReplyTo:       q.Name,
				})
			if err != nil {
				log.Error(err, "connection refused,reconnecting...")
				ch, q, msgs = rpcClient.getChannel()
				continue
			}
			for {
				timeout := make(chan bool, 1)
				go func() {
					time.Sleep(3 * time.Second) // timeout three second
					timeout <- true
				}()
				select {
				case d := <-msgs:
					if corrID == d.CorrelationId {
						rpcClient.ReplyChan <- d.Body
					} else {
						log.Errorf("msg correlationID : %s  invalide correlationID %s", corrID, d.CorrelationId)
					}
				case <-timeout:
					log.Errorf("mq rpc error : rpc reply timeout")
					goto End
				}
			}
		End:
			break
		}
	}
}

// Send Send Msg and waiting reply
func (rpcClient *RPCClient) Send(msg []byte) (reply []byte, err error) {
	rpcClient.Lock()
	defer rpcClient.Unlock()
	rpcClient.PayloadChan <- msg
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(3 * time.Second) // timeout three second
		timeout <- true
	}()
	select {
	case reply = <-rpcClient.ReplyChan:
		return reply, nil
	case <-timeout:
		return nil, errors.New("rpc reply time out")
	}
}

func (rpcClient *RPCClient) getChannel() (*amqp.Channel, amqp.Queue, <-chan amqp.Delivery) {
	var conn *amqp.Connection
	var ch *amqp.Channel
	var q amqp.Queue
	var msgs <-chan amqp.Delivery
	var err error
	for {
		conn, err = amqp.Dial(rpcClient.Config.URL())
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
		q, err = ch.QueueDeclare(
			rpcClient.Queue, // name
			true,            // durable
			false,           // delete when unused
			false,           // exclusive
			false,           // no-wait
			nil,             // arguments
		)
		if err != nil {
			log.Error(err, "Retry in 2 seconds")
			time.Sleep(time.Second * 2)
			continue
		}
		msgs, err = ch.Consume(
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
		return ch, q, msgs
	}
}
