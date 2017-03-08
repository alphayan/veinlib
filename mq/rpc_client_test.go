package mq

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

func TestRpcClient(t *testing.T) {
	log.Info("start RPCClient test")
	rpcClient := RPCClient{
		Config: Config{
			Username: "guest",
			Password: "guest",
			Host:     host,
			Port:     "5672",
		},
		Queue: "veinlib_test_rpc",
	}
	go rpcClient.Start()
	rpc := RPC{
		Config: Config{
			Username: "guest",
			Password: "guest",
			Host:     host,
			Port:     "5672",
		},
		Queue:     "veinlib_test_rpc",
		Processor: rpcReply,
	}
	go rpc.Start()
	time.Sleep(time.Second)
	log.Info("start sent rpc to veinlib_test_queue")
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		go func() {
			req := i
			wg.Add(1)
			reply, err := rpcClient.Send([]byte(strconv.Itoa(req)))
			if err != nil {
				t.Errorf("rpc_client_test error : %s", err)
			}
			res, err := strconv.Atoi(string(reply))
			if err != nil || res != req {
				t.Errorf("value changed:%d,%d", res, req)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func rpcReply(delivery amqp.Delivery) amqp.Publishing {
	reply := amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: delivery.CorrelationId,
		Body:          delivery.Body,
		Timestamp:     time.Now(),
	}
	time.Sleep(time.Microsecond * time.Duration(rand.Int31n(100)))
	return reply
}
